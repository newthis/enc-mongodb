/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb2.connection;

import com.mongodb2.MongoException;
import com.mongodb2.MongoInternalException;
import com.mongodb2.MongoInterruptedException;
import com.mongodb2.MongoSocketException;
import com.mongodb2.MongoSocketReadTimeoutException;
import com.mongodb2.MongoTimeoutException;
import com.mongodb2.MongoWaitQueueFullException;
import com.mongodb2.async.SingleResultCallback;
import com.mongodb2.diagnostics.logging.Logger;
import com.mongodb2.diagnostics.logging.Loggers;
import com.mongodb2.event.ConnectionAddedEvent;
import com.mongodb2.event.ConnectionCheckedInEvent;
import com.mongodb2.event.ConnectionCheckedOutEvent;
import com.mongodb2.event.ConnectionPoolClosedEvent;
import com.mongodb2.event.ConnectionPoolListener;
import com.mongodb2.event.ConnectionPoolOpenedEvent;
import com.mongodb2.event.ConnectionPoolWaitQueueEnteredEvent;
import com.mongodb2.event.ConnectionPoolWaitQueueExitedEvent;
import com.mongodb2.event.ConnectionRemovedEvent;
import com.mongodb2.internal.connection.ConcurrentPool;
import com.mongodb2.internal.thread.DaemonThreadFactory;
import org.bson2.ByteBuf;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb2.assertions.Assertions.isTrue;
import static com.mongodb2.assertions.Assertions.notNull;
import static com.mongodb2.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class DefaultConnectionPool implements ConnectionPool {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private static final DaemonThreadFactory THREAD_FACTORY = new DaemonThreadFactory();

    private final ConcurrentPool<UsageTrackingInternalConnection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);
    private final AtomicInteger generation = new AtomicInteger(0);
    private final ExecutorService sizeMaintenanceTimer;
    private ExecutorService asyncGetter;
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private final ServerId serverId;
    private volatile boolean closed;

    public DefaultConnectionPool(final ServerId serverId,
                                 final InternalConnectionFactory internalConnectionFactory, final ConnectionPoolSettings settings,
                                 final ConnectionPoolListener connectionPoolListener) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        UsageTrackingInternalConnectionItemFactory connectionItemFactory
        = new UsageTrackingInternalConnectionItemFactory(internalConnectionFactory);
        pool = new ConcurrentPool<UsageTrackingInternalConnection>(settings.getMaxSize(), connectionItemFactory);
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createTimer();
        this.connectionPoolListener = notNull("connectionPoolListener", connectionPoolListener);
        connectionPoolListener.connectionPoolOpened(new ConnectionPoolOpenedEvent(serverId, settings));
    }

    @Override
    public InternalConnection get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public InternalConnection get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw createWaitQueueFullException();
            }
            try {
                connectionPoolListener.waitQueueEntered(new ConnectionPoolWaitQueueEnteredEvent(serverId, currentThread().getId()));
                PooledConnection pooledConnection = getPooledConnection(timeout, timeUnit);
                if (!pooledConnection.opened()) {
                    try {
                        pooledConnection.open();
                    } catch (Throwable t) {
                        pool.release(pooledConnection.wrapped, true);
                        if (t instanceof MongoException) {
                            throw (MongoException) t;
                        } else {
                            throw new MongoInternalException(t.toString(), t);
                        }
                    }
                }

                return pooledConnection;
            } finally {
                connectionPoolListener.waitQueueExited(new ConnectionPoolWaitQueueExitedEvent(serverId, currentThread().getId()));
            }
        } finally {
            waitQueueSize.decrementAndGet();
        }
    }

    @Override
    public void getAsync(final SingleResultCallback<InternalConnection> callback) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Asynchronously getting a connection from the pool for server %s", serverId));
        }

        final SingleResultCallback<InternalConnection> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        PooledConnection connection = null;

        try {
            connection = getPooledConnection(0, MILLISECONDS);
        } catch (MongoTimeoutException e) {
            // fall through
        } catch (Throwable t) {
            callback.onResult(null, t);
            return;
        }

        if (connection != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Asynchronously opening pooled connection %s to server %s",
                                           connection.getDescription().getConnectionId(), serverId));
            }
            openAsync(connection, errHandlingCallback);
        } else if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
            waitQueueSize.decrementAndGet();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Asynchronously failing to get a pooled connection to %s because the wait queue is full",
                                           serverId));
            }
            callback.onResult(null, createWaitQueueFullException());
        } else {
            final long startTimeMillis = System.currentTimeMillis();
            connectionPoolListener.waitQueueEntered(new ConnectionPoolWaitQueueEnteredEvent(serverId, currentThread().getId()));
            getAsyncGetter().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (getRemainingWaitTime() <= 0) {
                            errHandlingCallback.onResult(null, createTimeoutException());
                        } else {
                            PooledConnection connection = getPooledConnection(getRemainingWaitTime(), MILLISECONDS);
                            openAsync(connection, errHandlingCallback);
                        }
                    } catch (Throwable t) {
                        errHandlingCallback.onResult(null, t);
                    } finally {
                        waitQueueSize.decrementAndGet();
                        connectionPoolListener.waitQueueExited(new ConnectionPoolWaitQueueExitedEvent(serverId, currentThread().getId()));
                    }
                }

                private long getRemainingWaitTime() {
                    return startTimeMillis + settings.getMaxWaitTime(MILLISECONDS) - System.currentTimeMillis();
                }
            });
        }
    }

    private void openAsync(final PooledConnection pooledConnection,
                           final SingleResultCallback<InternalConnection> callback) {
        if (pooledConnection.opened()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Pooled connection %s to server %s is already open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            callback.onResult(pooledConnection, null);
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("Pooled connection %s to server %s is not yet open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            pooledConnection.openAsync(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(String.format("Pooled connection %s to server %s failed to open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        callback.onResult(null, t);
                        pool.release(pooledConnection.wrapped, true);
                    } else {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(String.format("Pooled connection %s to server %s is now open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        callback.onResult(pooledConnection, null);
                    }
                }
            });
        }
    }

    private synchronized ExecutorService getAsyncGetter() {
        if (asyncGetter == null) {
            asyncGetter = Executors.newSingleThreadExecutor(THREAD_FACTORY);
        }
        return asyncGetter;
    }

    private synchronized void shutdownAsyncGetter() {
        if (asyncGetter != null) {
            asyncGetter.shutdownNow();
        }
    }

    @Override
    public void invalidate() {
        LOGGER.debug("Invalidating the connection pool");
        generation.incrementAndGet();
    }

    @Override
    public void close() {
        if (!closed) {
            pool.close();
            if (sizeMaintenanceTimer != null) {
                sizeMaintenanceTimer.shutdownNow();
            }
            shutdownAsyncGetter();
            closed = true;
            connectionPoolListener.connectionPoolClosed(new ConnectionPoolClosedEvent(serverId));
        }
    }

    /**
     * Synchronously prune idle connections and ensure the minimum pool size.
     */
    public void doMaintenance() {
        if (maintenanceTask != null) {
            maintenanceTask.run();
        }
    }

    private PooledConnection getPooledConnection(final long timeout, final TimeUnit timeUnit) {
        UsageTrackingInternalConnection internalConnection = pool.get(timeout, timeUnit);
        while (shouldPrune(internalConnection)) {
            pool.release(internalConnection, true);
            internalConnection = pool.get(timeout, timeUnit);
        }
        connectionPoolListener.connectionCheckedOut(new ConnectionCheckedOutEvent(internalConnection.getDescription().getConnectionId()));
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Checked out connection [%s] to server %s", getId(internalConnection), serverId.getAddress()));
        }
        return new PooledConnection(internalConnection);
    }

    private MongoTimeoutException createTimeoutException() {
        return new MongoTimeoutException(format("Timed out after %d ms while waiting for a connection to server %s.",
                                                settings.getMaxWaitTime(MILLISECONDS), serverId.getAddress()));
    }

    private MongoWaitQueueFullException createWaitQueueFullException() {
        return new MongoWaitQueueFullException(format("Too many threads are already waiting for a connection. "
                                                      + "Max number of threads (maxWaitQueueSize) of %d has been exceeded.",
                                                      settings.getMaxWaitQueueSize()));
    }

    ConcurrentPool<UsageTrackingInternalConnection> getPool() {
        return pool;
    }

    private Runnable createMaintenanceTask() {
        Runnable newMaintenanceTask = null;
        if (shouldPrune() || shouldEnsureMinSize()) {
            newMaintenanceTask = new Runnable() {
                @Override
                public synchronized void run() {
                    try {
                        if (shouldPrune()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Pruning pooled connections to %s", serverId.getAddress()));
                            }
                            pool.prune();
                        }
                        if (shouldEnsureMinSize()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Ensuring minimum pooled connections to %s", serverId.getAddress()));
                            }
                            pool.ensureMinSize(settings.getMinSize(), true);
                        }
                    } catch (MongoInterruptedException e) {
                        // don't log interruptions due to the shutdownNow call on the ExecutorService
                    } catch (Exception e) {
                        LOGGER.warn("Exception thrown during connection pool background maintenance task", e);
                    }
                }
            };
        }
        return newMaintenanceTask;
    }

    private ExecutorService createTimer() {
        if (maintenanceTask == null) {
            return null;
        } else {
            ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
            newTimer.scheduleAtFixedRate(maintenanceTask, settings.getMaintenanceInitialDelay(MILLISECONDS),
                                         settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS);
            return newTimer;
        }
    }

    private boolean shouldEnsureMinSize() {
        return settings.getMinSize() > 0;
    }

    private boolean shouldPrune() {
        return settings.getMaxConnectionIdleTime(MILLISECONDS) > 0 || settings.getMaxConnectionLifeTime(MILLISECONDS) > 0;
    }

    private boolean shouldPrune(final UsageTrackingInternalConnection connection) {
        return fromPreviousGeneration(connection) || pastMaxLifeTime(connection) || pastMaxIdleTime(connection);
    }

    private boolean pastMaxIdleTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getLastUsedAt(), System.currentTimeMillis(), settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean pastMaxLifeTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getOpenedAt(), System.currentTimeMillis(), settings.getMaxConnectionLifeTime(MILLISECONDS));
    }

    private boolean fromPreviousGeneration(final UsageTrackingInternalConnection connection) {
        return generation.get() > connection.getGeneration();
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    /**
     * If there was a socket exception that wasn't some form of interrupted read, increment the generation count so that any connections
     * created prior will be discarded.
     *
     * @param connection the connection that generated the exception
     * @param t          the exception
     */
    private void incrementGenerationOnSocketException(final InternalConnection connection, final Throwable t) {
        if (t instanceof MongoSocketException && !(t instanceof MongoSocketReadTimeoutException)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(format("Got socket exception on connection [%s] to %s. All connections to %s will be closed.",
                                   getId(connection), serverId.getAddress(), serverId.getAddress()));
            }
            invalidate();
        }
    }

    private ConnectionId getId(final InternalConnection internalConnection) {
        return internalConnection.getDescription().getConnectionId();
    }

    private class PooledConnection implements InternalConnection {
        private final UsageTrackingInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        public PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void open() {
            isTrue("open", !isClosed.get());
            wrapped.open();
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.openAsync(callback);
        }

        @Override
        public void close() {
            // All but the first call is a no-op
            if (!isClosed.getAndSet(true)) {
                if (!DefaultConnectionPool.this.closed) {
                    connectionPoolListener.connectionCheckedIn(new ConnectionCheckedInEvent(getId(wrapped)));
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace(format("Checked in connection [%s] to server %s", getId(wrapped), serverId.getAddress()));
                    }
                }
                pool.release(wrapped, wrapped.isClosed() || shouldPrune(wrapped));
            }
        }

        @Override
        public boolean opened() {
            isTrue("open", !isClosed.get());
            return wrapped.opened();
        }

        @Override
        public boolean isClosed() {
            return isClosed.get() || wrapped.isClosed();
        }

        @Override
        public ByteBuf getBuffer(final int capacity) {
            return wrapped.getBuffer(capacity);
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", !isClosed.get());
            try {
                wrapped.sendMessage(byteBuffers, lastRequestId);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", !isClosed.get());
            try {
                return wrapped.receiveMessage(responseTo);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        incrementGenerationOnSocketException(PooledConnection.this, t);
                    }
                    callback.onResult(null, t);
                }
            });
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed.get());
            wrapped.receiveMessageAsync(responseTo, new SingleResultCallback<ResponseBuffers>() {
                @Override
                public void onResult(final ResponseBuffers result, final Throwable t) {
                    if (t != null) {
                        incrementGenerationOnSocketException(PooledConnection.this, t);
                    }
                    callback.onResult(result, t);
                }
            });
        }

        @Override
        public ConnectionDescription getDescription() {
            isTrue("open", !isClosed.get());
            return wrapped.getDescription();
        }
    }

    private class UsageTrackingInternalConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingInternalConnection> {
        private final InternalConnectionFactory internalConnectionFactory;

        public UsageTrackingInternalConnectionItemFactory(final InternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public UsageTrackingInternalConnection create(final boolean initialize) {
            UsageTrackingInternalConnection internalConnection =
            new UsageTrackingInternalConnection(internalConnectionFactory.create(serverId), generation.get());
            if (initialize) {
                internalConnection.open();
            }
            connectionPoolListener.connectionAdded(new ConnectionAddedEvent(getId(internalConnection)));
            return internalConnection;
        }

        @Override
        public void close(final UsageTrackingInternalConnection connection) {
            if (!closed) {
                connectionPoolListener.connectionRemoved(new ConnectionRemovedEvent(getId(connection)));
            }
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Closed connection [%s] to %s because %s.", getId(connection), serverId.getAddress(),
                                  getReasonForClosing(connection)));
            }
            connection.close();
        }

        private String getReasonForClosing(final UsageTrackingInternalConnection connection) {
            String reason;
            if (connection.isClosed()) {
                reason = "there was a socket exception raised by this connection";
            } else if (fromPreviousGeneration(connection)) {
                reason = "there was a socket exception raised on another connection from this pool";
            } else if (pastMaxLifeTime(connection)) {
                reason = "it is past its maximum allowed life time";
            } else if (pastMaxIdleTime(connection)) {
                reason = "it is past its maximum allowed idle time";
            } else {
                reason = "the pool has been closed";
            }
            return reason;
        }

        @Override
        public boolean shouldPrune(final UsageTrackingInternalConnection usageTrackingConnection) {
            return DefaultConnectionPool.this.shouldPrune(usageTrackingConnection);
        }
    }
}

/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb2.MongoSocketException;
import com.mongodb2.annotations.ThreadSafe;
import com.mongodb2.diagnostics.logging.Logger;
import com.mongodb2.diagnostics.logging.Loggers;
import com.mongodb2.event.ServerHeartbeatFailedEvent;
import com.mongodb2.event.ServerHeartbeatStartedEvent;
import com.mongodb2.event.ServerHeartbeatSucceededEvent;
import com.mongodb2.event.ServerMonitorEventMulticaster;
import com.mongodb2.event.ServerMonitorListener;
import org.bson2.BsonDocument;
import org.bson2.BsonInt32;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb2.connection.CommandHelper.executeCommand;
import static com.mongodb2.connection.DescriptionHelper.createServerDescription;
import static com.mongodb2.connection.ServerConnectionState.CONNECTING;
import static com.mongodb2.connection.ServerType.UNKNOWN;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
class DefaultServerMonitor implements ServerMonitor {

    private static final Logger LOGGER = Loggers.getLogger("cluster");

    private final ServerId serverId;
    private final ServerMonitorListener serverMonitorListener;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final InternalConnectionFactory internalConnectionFactory;
    private final ConnectionPool connectionPool;
    private final ServerSettings settings;
    private final ServerMonitorRunnable monitor;
    private final Thread monitorThread;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private volatile boolean isClosed;

    DefaultServerMonitor(final ServerId serverId, final ServerSettings settings,
                         final ChangeListener<ServerDescription> serverStateListener,
                         final InternalConnectionFactory internalConnectionFactory, final ConnectionPool connectionPool) {
        this.settings = settings;
        this.serverId = serverId;
        this.serverMonitorListener = settings.getServerMonitorListeners().isEmpty()
                                             ? new NoOpServerMonitorListener()
                                             : new ServerMonitorEventMulticaster(settings.getServerMonitorListeners());
        this.serverStateListener = serverStateListener;
        this.internalConnectionFactory = internalConnectionFactory;
        this.connectionPool = connectionPool;
        monitor = new ServerMonitorRunnable();
        monitorThread = new Thread(monitor, "cluster-" + this.serverId.getClusterId() + "-" + this.serverId.getAddress());
        monitorThread.setDaemon(true);
        isClosed = false;
    }

    @Override
    public void start() {
        monitorThread.start();
    }

    @Override
    public void connect() {
        lock.lock();
        try {
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        isClosed = true;
        monitorThread.interrupt();
    }

    class ServerMonitorRunnable implements Runnable {
        private final ExponentiallyWeightedMovingAverage averageRoundTripTime = new ExponentiallyWeightedMovingAverage(0.2);

        @Override
        @SuppressWarnings("unchecked")
        public synchronized void run() {
            InternalConnection connection = null;
            try {
                ServerDescription currentServerDescription = getConnectingServerDescription(null);
                while (!isClosed) {
                    ServerDescription previousServerDescription = currentServerDescription;
                    try {
                        if (connection == null) {
                            connection = internalConnectionFactory.create(serverId);
                            try {
                                connection.open();
                            } catch (Throwable t) {
                                connection = null;
                                throw t;
                            }
                        }
                        try {
                            currentServerDescription = lookupServerDescription(connection);
                        } catch (MongoSocketException e) {
                            connectionPool.invalidate();
                            connection.close();
                            connection = null;
                            connection = internalConnectionFactory.create(serverId);
                            try {
                                connection.open();
                            } catch (Throwable t) {
                                connection = null;
                                throw t;
                            }
                            try {
                                currentServerDescription = lookupServerDescription(connection);
                            } catch (MongoSocketException e1) {
                                connection.close();
                                connection = null;
                                throw e1;
                            }
                        }
                    } catch (Throwable t) {
                        averageRoundTripTime.reset();
                        currentServerDescription = getConnectingServerDescription(t);
                    }

                    if (!isClosed) {
                        try {
                            logStateChange(previousServerDescription, currentServerDescription);
                            serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(previousServerDescription,
                                                                                                       currentServerDescription));
                        } catch (Throwable t) {
                            LOGGER.warn("Exception in monitor thread during notification of server description state change", t);
                        }
                        waitForNext();
                    }
                }
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }

        private ServerDescription getConnectingServerDescription(final Throwable exception) {
            return ServerDescription.builder().type(UNKNOWN).state(CONNECTING).address(serverId.getAddress()).exception(exception).build();
        }

        private ServerDescription lookupServerDescription(final InternalConnection connection) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(format("Checking status of %s", serverId.getAddress()));
            }
            serverMonitorListener.serverHearbeatStarted(new ServerHeartbeatStartedEvent(connection.getDescription().getConnectionId()));

            long start = System.nanoTime();
            try {
                BsonDocument isMasterResult = executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)), connection);
                long elapsedTimeNanos = System.nanoTime() - start;
                averageRoundTripTime.addSample(elapsedTimeNanos);

                serverMonitorListener.serverHeartbeatSucceeded(
                        new ServerHeartbeatSucceededEvent(connection.getDescription().getConnectionId(), isMasterResult, elapsedTimeNanos));

                return createServerDescription(serverId.getAddress(), isMasterResult, connection.getDescription().getServerVersion(),
                                               averageRoundTripTime.getAverage());
            } catch (RuntimeException e) {
                serverMonitorListener.serverHeartbeatFailed(
                        new ServerHeartbeatFailedEvent(connection.getDescription().getConnectionId(), System.nanoTime() - start, e));
                throw e;
            }
        }

        private void logStateChange(final ServerDescription previousServerDescription,
                                    final ServerDescription currentServerDescription) {
            if (shouldLogStageChange(previousServerDescription, currentServerDescription)) {
                if (currentServerDescription.getException() != null) {
                    LOGGER.info(format("Exception in monitor thread while connecting to server %s", serverId.getAddress()),
                                currentServerDescription.getException());
                } else {
                    LOGGER.info(format("Monitor thread successfully connected to server with description %s", currentServerDescription));
                }
            }
        }

        private void waitForNext() {
            try {
                long timeRemaining = waitForSignalOrTimeout();
                if (timeRemaining > 0) {
                    long timeWaiting = settings.getHeartbeatFrequency(NANOSECONDS) - timeRemaining;
                    long minimumNanosToWait = settings.getMinHeartbeatFrequency(NANOSECONDS);
                    if (timeWaiting < minimumNanosToWait) {
                        long millisToSleep = MILLISECONDS.convert(minimumNanosToWait - timeWaiting, NANOSECONDS);
                        if (millisToSleep > 0) {
                            Thread.sleep(millisToSleep);
                        }
                    }
                }
            } catch (InterruptedException e) {
                // fall through
            }
        }

        private long waitForSignalOrTimeout() throws InterruptedException {
            lock.lock();
            try {
                return condition.awaitNanos(settings.getHeartbeatFrequency(NANOSECONDS));
            } finally {
                lock.unlock();
            }
        }
    }

    static boolean shouldLogStageChange(final ServerDescription previous, final ServerDescription current) {

        if (previous.isOk() != current.isOk()) {
            return true;
        }
        if (!previous.getAddress().equals(current.getAddress())) {
            return true;
        }
        if (previous.getCanonicalAddress() != null
                    ? !previous.getCanonicalAddress().equals(current.getCanonicalAddress()) : current.getCanonicalAddress() != null) {
            return true;
        }
        if (!previous.getHosts().equals(current.getHosts())) {
            return true;
        }
        if (!previous.getArbiters().equals(current.getArbiters())) {
            return true;
        }
        if (!previous.getPassives().equals(current.getPassives())) {
            return true;
        }
        if (previous.getPrimary() != null ? !previous.getPrimary().equals(current.getPrimary()) : current.getPrimary() != null) {
            return true;
        }
        if (previous.getSetName() != null ? !previous.getSetName().equals(current.getSetName()) : current.getSetName() != null) {
            return true;
        }
        if (previous.getState() != current.getState()) {
            return true;
        }
        if (!previous.getTagSet().equals(current.getTagSet())) {
            return true;
        }
        if (previous.getType() != current.getType()) {
            return true;
        }
        if (!previous.getVersion().equals(current.getVersion())) {
            return true;
        }
        if (previous.getElectionId() != null
                    ? !previous.getElectionId().equals(current.getElectionId()) : current.getElectionId() != null) {
            return true;
        }
        if (previous.getSetVersion() != null
                    ? !previous.getSetVersion().equals(current.getSetVersion()) : current.getSetVersion() != null) {
            return true;
        }

        // Compare class equality and message as exceptions rarely override equals
        Class<?> thisExceptionClass = previous.getException() != null ? previous.getException().getClass() : null;
        Class<?> thatExceptionClass = current.getException() != null ? current.getException().getClass() : null;
        if (thisExceptionClass != null ? !thisExceptionClass.equals(thatExceptionClass) : thatExceptionClass != null) {
            return true;
        }

        String thisExceptionMessage = previous.getException() != null ? previous.getException().getMessage() : null;
        String thatExceptionMessage = current.getException() != null ? current.getException().getMessage() : null;
        if (thisExceptionMessage != null ? !thisExceptionMessage.equals(thatExceptionMessage) : thatExceptionMessage != null) {
            return true;
        }

        return false;
    }
}

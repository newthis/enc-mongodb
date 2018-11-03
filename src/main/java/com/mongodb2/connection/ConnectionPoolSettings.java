/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb2.ConnectionString;
import com.mongodb2.annotations.Immutable;
import com.mongodb2.annotations.NotThreadSafe;

import java.util.concurrent.TimeUnit;

import static com.mongodb2.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * All settings that relate to the pool of connections to a MongoDB server.
 *
 * @since 3.0
 */
@Immutable
public class ConnectionPoolSettings {
    private final int maxSize;
    private final int minSize;
    private final int maxWaitQueueSize;
    private final long maxWaitTimeMS;
    private final long maxConnectionLifeTimeMS;
    private final long maxConnectionIdleTimeMS;
    private final long maintenanceInitialDelayMS;
    private final long maintenanceFrequencyMS;

    /**
     * Gets a Builder for creating a new ConnectionPoolSettings instance.
     *
     * @return a new Builder for ConnectionPoolSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for creating ConnectionPoolSettings.
     */
    @NotThreadSafe
    public static class Builder {
        private int maxSize = 100;
        private int minSize;
        private int maxWaitQueueSize = 500;
        private long maxWaitTimeMS = 1000 * 60 * 2;
        private long maxConnectionLifeTimeMS;
        private long maxConnectionIdleTimeMS;
        private long maintenanceInitialDelayMS;
        private long maintenanceFrequencyMS = MILLISECONDS.convert(1, MINUTES);

        /**
         * <p>The maximum number of connections allowed. Those connections will be kept in the pool when idle. Once the pool is exhausted,
         * any operation requiring a connection will block waiting for an available connection.</p>
         *
         * <p>Default is 100.</p>
         *
         * @param maxSize the maximum number of connections in the pool.
         * @return this
         */
        public Builder maxSize(final int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        /**
         * <p>The minimum number of connections. Those connections will be kept in the pool when idle, and the pool will ensure that it
         * contains at least this minimum number.</p>
         *
         * <p>Default is 0.</p>
         *
         * @param minSize the minimum number of connections to have in the pool at all times.
         * @return this
         */
        public Builder minSize(final int minSize) {
            this.minSize = minSize;
            return this;
        }

        /**
         * <p>This is the maximum number of waiters for a connection to become available from the pool. All further operations will get an
         * exception immediately.</p>
         *
         * <p>Default is 500.</p>
         *
         * @param maxWaitQueueSize the number of threads that are allowed to be waiting for a connection.
         * @return this
         */
        public Builder maxWaitQueueSize(final int maxWaitQueueSize) {
            this.maxWaitQueueSize = maxWaitQueueSize;
            return this;
        }

        /**
         * <p>The maximum time that a thread may wait for a connection to become available.</p>
         *
         * <p>Default is 2 minutes. A value of 0 means that it will not wait.  A negative value means it will wait indefinitely.</p>
         *
         * @param maxWaitTime the maximum amount of time to wait
         * @param timeUnit    the TimeUnit for this wait period
         * @return this
         */
        public Builder maxWaitTime(final long maxWaitTime, final TimeUnit timeUnit) {
            this.maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
            return this;
        }

        /**
         * The maximum time a pooled connection can live for.  A zero value indicates no limit to the life time.  A pooled connection that
         * has exceeded its life time will be closed and replaced when necessary by a new connection.
         *
         * @param maxConnectionLifeTime the maximum length of time a connection can live
         * @param timeUnit              the TimeUnit for this time period
         * @return this
         */
        public Builder maxConnectionLifeTime(final long maxConnectionLifeTime, final TimeUnit timeUnit) {
            this.maxConnectionLifeTimeMS = MILLISECONDS.convert(maxConnectionLifeTime, timeUnit);
            return this;
        }

        /**
         * The maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that has
         * exceeded its idle time will be closed and replaced when necessary by a new connection.
         *
         * @param maxConnectionIdleTime the maximum time a connection can be unused
         * @param timeUnit              the TimeUnit for this time period
         * @return this
         */
        public Builder maxConnectionIdleTime(final long maxConnectionIdleTime, final TimeUnit timeUnit) {
            this.maxConnectionIdleTimeMS = MILLISECONDS.convert(maxConnectionIdleTime, timeUnit);
            return this;
        }

        /**
         * The period of time to wait before running the first maintenance job on the connection pool.
         *
         * @param maintenanceInitialDelay the time period to wait
         * @param timeUnit                the TimeUnit for this time period
         * @return this
         */
        public Builder maintenanceInitialDelay(final long maintenanceInitialDelay, final TimeUnit timeUnit) {
            this.maintenanceInitialDelayMS = MILLISECONDS.convert(maintenanceInitialDelay, timeUnit);
            return this;
        }

        /**
         * The time period between runs of the maintenance job.
         *
         * @param maintenanceFrequency the time period between runs of the maintenance job
         * @param timeUnit             the TimeUnit for this time period
         * @return this
         */
        public Builder maintenanceFrequency(final long maintenanceFrequency, final TimeUnit timeUnit) {
            this.maintenanceFrequencyMS = MILLISECONDS.convert(maintenanceFrequency, timeUnit);
            return this;
        }

        /**
         * Creates a new ConnectionPoolSettings object with the settings initialised on this builder.
         *
         * @return a new ConnectionPoolSettings object
         */
        public ConnectionPoolSettings build() {
            return new ConnectionPoolSettings(this);
        }

        /**
         * Takes connection pool settings from the given connection string and applies them to this builder.
         *
         * @param connectionString a URL with details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            if (connectionString.getMaxConnectionPoolSize() != null) {
                maxSize(connectionString.getMaxConnectionPoolSize());
            }
            if (connectionString.getMinConnectionPoolSize() != null) {
                minSize(connectionString.getMinConnectionPoolSize());
            }
            if (connectionString.getMaxWaitTime() != null) {
                maxWaitTime(connectionString.getMaxWaitTime(), MILLISECONDS);
            }
            if (connectionString.getMaxConnectionIdleTime() != null) {
                maxConnectionIdleTime(connectionString.getMaxConnectionIdleTime(), MILLISECONDS);
            }
            if (connectionString.getMaxConnectionLifeTime() != null) {
                maxConnectionLifeTime(connectionString.getMaxConnectionLifeTime(), MILLISECONDS);
            }
            if (connectionString.getThreadsAllowedToBlockForConnectionMultiplier() != null) {
                maxWaitQueueSize(connectionString.getThreadsAllowedToBlockForConnectionMultiplier() * maxSize);
            }
            return this;
        }
    }

    /**
     * <p>The maximum number of connections allowed. Those connections will be kept in the pool when idle. Once the pool is exhausted, any
     * operation requiring a connection will block waiting for an available connection.</p>
     *
     * <p>Default is 100.</p>
     *
     * @return the maximum number of connections in the pool.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * <p>The minimum number of connections. Those connections will be kept in the pool when idle, and the pool will ensure that it contains
     * at least this minimum number.</p>
     *
     * <p>Default is 0.</p>
     *
     * @return the minimum number of connections to have in the pool at all times.
     */
    public int getMinSize() {
        return minSize;
    }

    /**
     * <p>This is the maximum number of operations that may be waiting for a connection to become available from the pool. All further
     * operations will get an exception immediately.</p>
     *
     * <p>Default is 500.</p>
     *
     * @return the number of threads that are allowed to be waiting for a connection.
     */
    public int getMaxWaitQueueSize() {
        return maxWaitQueueSize;
    }

    /**
     * <p>The maximum time that a thread may wait for a connection to become available.</p>
     *
     * <p>Default is 2 minutes. A value of 0 means that it will not wait.  A negative value means it will wait indefinitely.</p>
     *
     * @param timeUnit the TimeUnit for this wait period
     * @return the maximum amount of time to wait in the given TimeUnits
     */
    public long getMaxWaitTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxWaitTimeMS, MILLISECONDS);
    }

    /**
     * The maximum time a pooled connection can live for.  A zero value indicates no limit to the life time.  A pooled connection that has
     * exceeded its life time will be closed and replaced when necessary by a new connection.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the maximum length of time a connection can live in the given TimeUnits
     */
    public long getMaxConnectionLifeTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxConnectionLifeTimeMS, MILLISECONDS);
    }

    /**
     * Returns the maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that
     * has exceeded its idle time will be closed and replaced when necessary by a new connection.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the maximum time a connection can be unused, in the given TimeUnits
     */
    public long getMaxConnectionIdleTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxConnectionIdleTimeMS, MILLISECONDS);
    }

    /**
     * Returns the period of time to wait before running the first maintenance job on the connection pool.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the time period to wait in the given units
     */
    public long getMaintenanceInitialDelay(final TimeUnit timeUnit) {
        return timeUnit.convert(maintenanceInitialDelayMS, MILLISECONDS);
    }

    /**
     * Returns the time period between runs of the maintenance job.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the time period between runs of the maintainance job in the given units
     */
    public long getMaintenanceFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(maintenanceFrequencyMS, MILLISECONDS);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionPoolSettings that = (ConnectionPoolSettings) o;

        if (maxConnectionIdleTimeMS != that.maxConnectionIdleTimeMS) {
            return false;
        }
        if (maxConnectionLifeTimeMS != that.maxConnectionLifeTimeMS) {
            return false;
        }
        if (maxSize != that.maxSize) {
            return false;
        }
        if (minSize != that.minSize) {
            return false;
        }
        if (maintenanceInitialDelayMS != that.maintenanceInitialDelayMS) {
            return false;
        }
        if (maintenanceFrequencyMS != that.maintenanceFrequencyMS) {
            return false;
        }
        if (maxWaitQueueSize != that.maxWaitQueueSize) {
            return false;
        }
        if (maxWaitTimeMS != that.maxWaitTimeMS) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = maxSize;
        result = 31 * result + minSize;
        result = 31 * result + maxWaitQueueSize;
        result = 31 * result + (int) (maxWaitTimeMS ^ (maxWaitTimeMS >>> 32));
        result = 31 * result + (int) (maxConnectionLifeTimeMS ^ (maxConnectionLifeTimeMS >>> 32));
        result = 31 * result + (int) (maxConnectionIdleTimeMS ^ (maxConnectionIdleTimeMS >>> 32));
        result = 31 * result + (int) (maintenanceInitialDelayMS ^ (maintenanceInitialDelayMS >>> 32));
        result = 31 * result + (int) (maintenanceFrequencyMS ^ (maintenanceFrequencyMS >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionPoolSettings{"
               + "maxSize=" + maxSize
               + ", minSize=" + minSize
               + ", maxWaitQueueSize=" + maxWaitQueueSize
               + ", maxWaitTimeMS=" + maxWaitTimeMS
               + ", maxConnectionLifeTimeMS=" + maxConnectionLifeTimeMS
               + ", maxConnectionIdleTimeMS=" + maxConnectionIdleTimeMS
               + ", maintenanceInitialDelayMS=" + maintenanceInitialDelayMS
               + ", maintenanceFrequencyMS=" + maintenanceFrequencyMS
               + '}';
    }

    ConnectionPoolSettings(final Builder builder) {
        isTrue("maxSize > 0", builder.maxSize > 0);
        isTrue("minSize >= 0", builder.minSize >= 0);
        isTrue("maxWaitQueueSize >= 0", builder.maxWaitQueueSize >= 0);
        isTrue("maintenanceInitialDelayMS >= 0", builder.maintenanceInitialDelayMS >= 0);
        isTrue("maxConnectionLifeTime >= 0", builder.maxConnectionLifeTimeMS >= 0);
        isTrue("maxConnectionIdleTime >= 0", builder.maxConnectionIdleTimeMS >= 0);
        isTrue("sizeMaintenanceFrequency > 0", builder.maintenanceFrequencyMS > 0);
        isTrue("maxSize >= minSize", builder.maxSize >= builder.minSize);

        maxSize = builder.maxSize;
        minSize = builder.minSize;
        maxWaitQueueSize = builder.maxWaitQueueSize;
        maxWaitTimeMS = builder.maxWaitTimeMS;
        maxConnectionLifeTimeMS = builder.maxConnectionLifeTimeMS;
        maxConnectionIdleTimeMS = builder.maxConnectionIdleTimeMS;
        maintenanceInitialDelayMS = builder.maintenanceInitialDelayMS;
        maintenanceFrequencyMS = builder.maintenanceFrequencyMS;
    }
}

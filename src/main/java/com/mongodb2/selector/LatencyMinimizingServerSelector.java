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

package com.mongodb2.selector;

import com.mongodb2.connection.ClusterDescription;
import com.mongodb2.connection.ServerDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb2.connection.ClusterConnectionMode.MULTIPLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A server selector that accepts only servers within the given ping-time latency difference from the faster of the servers.
 *
 * @since 3.0
 */
public class LatencyMinimizingServerSelector implements ServerSelector {

    private final long acceptableLatencyDifferenceNanos;

    /**
     *
     * @param acceptableLatencyDifference the maximum difference in ping-time latency between the fastest ping time and the slowest of
     *                                    the chosen servers
     * @param timeUnit the time unit of the acceptableLatencyDifference
     */
    public LatencyMinimizingServerSelector(final long acceptableLatencyDifference, final TimeUnit timeUnit) {
        this.acceptableLatencyDifferenceNanos = NANOSECONDS.convert(acceptableLatencyDifference, timeUnit);
    }

    /**
     * Gets the acceptable latency difference.
     *
     * @param timeUnit the time unit to get it in.
     * @return the acceptable latency difference in the specified time unit
     */
    public long getAcceptableLatencyDifference(final TimeUnit timeUnit) {
        return timeUnit.convert(acceptableLatencyDifferenceNanos, NANOSECONDS);
    }

    @Override
    @SuppressWarnings("deprecation")
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() != MULTIPLE) {
            return clusterDescription.getAny();
        } else {
            return getServersWithAcceptableLatencyDifference(clusterDescription.getAny(),
                                                             getFastestRoundTripTimeNanos(clusterDescription.getServerDescriptions()));
        }
    }

    @Override
    public String toString() {
        return "LatencyMinimizingServerSelector{"
               + "acceptableLatencyDifference=" + MILLISECONDS.convert(acceptableLatencyDifferenceNanos, NANOSECONDS) + " ms"
               + '}';
    }

    private long getFastestRoundTripTimeNanos(final List<ServerDescription> members) {
        long fastestRoundTripTime = Long.MAX_VALUE;
        for (final ServerDescription cur : members) {
            if (!cur.isOk()) {
                continue;
            }
            if (cur.getRoundTripTimeNanos() < fastestRoundTripTime) {
                fastestRoundTripTime = cur.getRoundTripTimeNanos();
            }
        }
        return fastestRoundTripTime;
    }

    private List<ServerDescription> getServersWithAcceptableLatencyDifference(final List<ServerDescription> servers,
                                                                              final long bestPingTime) {
        List<ServerDescription> goodSecondaries = new ArrayList<ServerDescription>(servers.size());
        for (final ServerDescription cur : servers) {
            if (!cur.isOk()) {
                continue;
            }
            if (cur.getRoundTripTimeNanos() - acceptableLatencyDifferenceNanos <= bestPingTime) {
                goodSecondaries.add(cur);
            }
        }
        return goodSecondaries;
    }
}

/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mpp.operator;

import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.mpp.execution.SystemMemoryUsageListener;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import io.airlift.http.client.HttpClient;
import io.airlift.units.DataSize;

import javax.inject.Inject;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

public class ExchangeClientFactory implements ExchangeClientSupplier {
    private final int concurrentRequestMultiplier;
    private final long minErrorDuration;
    private final long maxErrorDuration;
    private final HttpClient httpClient;
    private final DataSize maxResponseSize;
    private final ScheduledExecutorService executor;

    @Inject
    public ExchangeClientFactory(
        @ForExchange HttpClient httpClient,
        @ForExchange ScheduledExecutorService executor) {
        this(MppConfig.getInstance().getExchangeMaxResponseSize(),
            MppConfig.getInstance().getExchangeConcurrentRequestMultiplier(),
            MppConfig.getInstance().getExchangeMinErrorDuration(),
            MppConfig.getInstance().getExchangeMaxErrorDuration(),
            httpClient,
            executor);
    }

    public ExchangeClientFactory(
        long maxResponseSize,
        int concurrentRequestMultiplier,
        long minErrorDuration,
        long maxErrorDuration,
        HttpClient httpClient,
        ScheduledExecutorService executor) {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.minErrorDuration = minErrorDuration;
        this.maxErrorDuration = maxErrorDuration;
        this.httpClient = requireNonNull(httpClient, "httpClient is null");

        // Use only 0.75 of the maxResponseSize to leave room for additional bytes from the encoding
        // TODO figure out a better way to compute the size of data that will be transferred over the network
        long maxResponseSizeBytes =
            (long) (Math.min(httpClient.getMaxContentLength(), maxResponseSize) * 0.75);
        this.maxResponseSize = new DataSize(maxResponseSizeBytes, BYTE);

        this.executor = requireNonNull(executor, "executor is null");
        checkArgument(maxResponseSize > 0, "maxResponseSize must be at least 1 byte: %s", maxResponseSize);
        checkArgument(concurrentRequestMultiplier > 0, "concurrentRequestMultiplier must be at least 1: %s",
            concurrentRequestMultiplier);
    }

    @Override
    public ExchangeClient get(SystemMemoryUsageListener systemMemoryUsageListener, ExecutionContext context) {
        return new ExchangeClient(
            context,
            maxResponseSize,
            concurrentRequestMultiplier,
            minErrorDuration,
            maxErrorDuration,
            httpClient,
            executor,
            systemMemoryUsageListener);
    }
}

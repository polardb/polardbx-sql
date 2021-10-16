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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LocalExchangersStatus {

    private final List<AtomicBoolean> consumings = new ArrayList<>();
    private AtomicInteger buildCount = new AtomicInteger(0);
    private AtomicInteger currentParallelism = new AtomicInteger(0);
    private int consumerParallelism;

    public final AtomicBoolean openAllConsume = new AtomicBoolean(false);

    public final AtomicBoolean closedAllConsume = new AtomicBoolean(false);

    public LocalExchangersStatus(int consumerParallelism) {
        for (int i = 0; i < consumerParallelism; i++) {
            consumings.add(new AtomicBoolean(false));
        }
        this.consumerParallelism = consumerParallelism;
    }

    public void incrementParallelism() {
        currentParallelism.incrementAndGet();
    }

    public int getCurrentParallelism() {
        return currentParallelism.get();
    }

    public int getConsumerParallelism() {
        return consumerParallelism;
    }

    public List<AtomicBoolean> getConsumings() {
        return consumings;
    }

    public int getBuildCount() {
        return buildCount.incrementAndGet();
    }
}

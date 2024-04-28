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

package com.alibaba.polardbx.executor.statistic.ndv;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FlowControl {
    // static
    private static Map<String, FlowControl> flowControlMap = Maps.newConcurrentMap();
    private static long TURN_TIME = 1000 * 60 * 5;
    private static long LIMIT_TIME = 1000 * 60 * 1;

    /**
     * there must being built before getInstance
     */
    public static FlowControl getInstance(String key) {
        if (flowControlMap.get(key) == null) {
            FlowControl flowControl = new FlowControl();
            flowControlMap.put(key, flowControl);
            return flowControl;
        }
        return flowControlMap.get(key);
    }
    // static end

    private volatile AtomicBoolean isRunningCurrent = new AtomicBoolean(false);
    private AtomicLong currentTime = new AtomicLong();
    private volatile long currentTurn = -1L;

    public FlowControl() {
    }

    public boolean acquire() {
        if (isRunningCurrent.get()) {
            return false;
        }
        // check time update
        checkCurrentTime();
        if (currentTime.get() > LIMIT_TIME) {
            return false;
        } else {
            isRunningCurrent.set(true);
            return true;
        }
    }

    public void feedback(long feedTime) {
        isRunningCurrent.set(false);
        // check time update
        checkCurrentTime();
        // first update turn time, then add time.
        currentTime.addAndGet(feedTime);
    }

    // refresh time every turn
    private void checkCurrentTime() {
        long currentTurnTmp = System.currentTimeMillis() / TURN_TIME;
        if (currentTurnTmp != currentTurn) {
            currentTurn = currentTurnTmp;
            currentTime.set(0);
        }
    }

}

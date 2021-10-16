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

package com.alibaba.polardbx.optimizer.ccl.common;

import lombok.Data;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author busu
 * date: 2020/10/30 5:35 下午
 */
@Data
public class CclContext {

    private static final int INITIAL_STATE = 0;
    private static final int READY_STATE = 1;
    private static final int FINISH_STATE = 2;

    private CclRuleInfo cclRule;
    private Thread thread;
    private volatile boolean valid;
    private AtomicInteger ready;
    private boolean hitCache;
    private boolean reschedule;

    private CclMetric metric;

    public CclContext(CclRuleInfo cclRule, Thread thread) {
        this.cclRule = cclRule;
        this.thread = thread;
        this.valid = true;
        this.ready = new AtomicInteger(INITIAL_STATE);
    }

    public CclContext(boolean hitCache) {
        this.hitCache = hitCache;
        this.valid = true;
        this.ready = new AtomicInteger(INITIAL_STATE);
    }

    public boolean isReady() {
        return this.ready.get() == READY_STATE;
    }

    public boolean setReady() {
        return this.ready.compareAndSet(INITIAL_STATE, READY_STATE);
    }

    public boolean setFinish() {
        while (this.ready.get() != FINISH_STATE) {
            boolean result = this.ready.compareAndSet(READY_STATE, FINISH_STATE);
            if (result) {
                return true;
            }
            this.ready.compareAndSet(INITIAL_STATE, FINISH_STATE);
        }
        return false;
    }

}

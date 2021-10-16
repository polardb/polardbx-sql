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

/**
 * @author busu
 * date: 2020/11/2 6:53 下午
 */
@Data
public class CclMetric {

    public final static int KILLED = 1;
    public final static int WAIT = 2;
    public final static int RUN = 3;
    public final static int WAIT_K = 4;
    public final static int RESCHEDULE = 5;

    private final int type;
    private final long value;
    private final String ruleName;
    private final boolean hitCache;

    public CclMetric(int type, long value, String ruleName, boolean hitCache) {
        this.type = type;
        this.value = value;
        this.ruleName = ruleName;
        this.hitCache = hitCache;
    }

    public CclMetric(int type, String ruleName, boolean hitCache) {
        this.type = type;
        this.value = 0;
        this.ruleName = ruleName;
        this.hitCache = hitCache;
    }

}

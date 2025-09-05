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

import com.alibaba.polardbx.optimizer.ccl.service.Reschedulable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author busu
 * date: 2021/2/20 1:45 下午
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class RescheduleTask {
    /**
     * The operation object
     */
    private Reschedulable reschedulable;
    /**
     * the start timestamp of reschedule waiting, unit: ms
     */
    private long waitStartTs;
    /**
     * the end timestamp of reschedule waiting, unit: ms
     */
    private long waitEndTs;
    /**
     * concurrency control rule information.
     */
    private CclRuleInfo<RescheduleTask> cclRuleInfo;
    /**
     * if hit the ccl match cache.
     */
    private boolean hitCache;

    /**
     * Is switchover reschedule task.
     */
    private boolean isSwitchoverReschedule;

    /**
     * 是否被激活
     */
    private AtomicBoolean activation;
}

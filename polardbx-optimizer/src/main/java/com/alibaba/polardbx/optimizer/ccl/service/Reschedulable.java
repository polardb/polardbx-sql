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

package com.alibaba.polardbx.optimizer.ccl.service;

import com.alibaba.polardbx.optimizer.ccl.common.CclRuleInfo;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;

import java.util.function.Function;

/**
 * @author busu
 * date: 2021/2/18 7:11 下午
 */
public interface Reschedulable {

    /**
     * 重新调度
     */
    boolean reschedule(Function<CclRuleInfo<RescheduleTask>, Boolean> function);

    /**
     * 是否正在被重新调度
     */
    boolean isRescheduled();

    /**
     * 设置reschedule的状态
     */
    void setRescheduled(boolean rescheduled, RescheduleTask rescheduleTask);

    /**
     * 处理异常
     */
    void handleRescheduleError(Throwable throwable);

    /**
     * 检查是否健康
     */
    boolean isCanReschedule();

    /**
     * 获得重新调度的任务
     */
    RescheduleTask getRescheduleTask();

}

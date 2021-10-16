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

package com.alibaba.polardbx.optimizer.ccl.exception;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleCallback;
import lombok.Data;

/**
 * @author busu
 * date: 2021/2/22 9:02 下午
 */
@Data
public class CclRescheduleException extends TddlNestableRuntimeException {

    private RescheduleCallback rescheduleCallback;

    public CclRescheduleException(String msg, RescheduleCallback rescheduleCallback) {
        super(msg);
        this.rescheduleCallback = rescheduleCallback;
    }

}

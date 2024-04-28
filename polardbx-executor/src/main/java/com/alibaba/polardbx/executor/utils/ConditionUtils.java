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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.utils.GeneralUtil;

/**
 * Created by chuanqin on 18/3/27.
 */
public class ConditionUtils {
    public static boolean convertConditionToBoolean(Object condition) {
        if (condition instanceof Boolean) {
            return (boolean) condition;
        } else if (condition instanceof Number) {
            return ((Number) condition).longValue() != 0L;
        } else if (condition == null) {
            return false;
        } else {
            GeneralUtil.nestedException("Illegal condition value: " + condition.getClass());
        }
        return false;
    }
}
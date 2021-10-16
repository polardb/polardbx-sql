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

package com.alibaba.polardbx.optimizer.core.datatype;

public class BooleanType extends IntegerType {

    public static boolean isTrue(Integer i) {
        if (i == null) {
            return false;
        }

        return i != 0;
    }

    @Override
    public Integer getMaxValue() {
        return 127; // 1代表true
    }

    @Override
    public Integer getMinValue() {
        return -128; // 0代表false
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.BOOLEAN;
    }

}

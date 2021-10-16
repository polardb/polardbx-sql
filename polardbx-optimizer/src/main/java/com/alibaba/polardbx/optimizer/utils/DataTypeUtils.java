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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

public class DataTypeUtils {

    public static Object convert(DataType toType, Object value) {
        if (toType == null) {
            // Special cases such as compound types
            return value;
        }
        return toType.convertFrom(value);
    }

    public static DataType[] gather(DataType[] inputTypes, int[] indexes) {
        DataType[] result = new DataType[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            result[i] = inputTypes[indexes[i]];
        }
        return result;
    }
}

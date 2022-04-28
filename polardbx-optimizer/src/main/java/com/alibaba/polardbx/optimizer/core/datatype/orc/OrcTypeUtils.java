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

package com.alibaba.polardbx.optimizer.core.datatype.orc;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;

public class OrcTypeUtils {
    // todo
    public static List<DataType> parseFrom(TypeDescription typeDescription) {
        List<DataType> dataTypeList = new ArrayList<>();
        for (int column = 1; column <= typeDescription.getMaximumId(); column++) {
            TypeDescription subType = typeDescription.findSubtype(column);
            switch (subType.getCategory()) {
            case BYTE:
                dataTypeList.add(DataTypes.TinyIntType);
                break;
            case SHORT:
                dataTypeList.add(DataTypes.ShortType);
                break;
            case INT:
                dataTypeList.add(DataTypes.IntegerType);
                break;
            case LONG:
                break;
            }
        }
        return dataTypeList;
    }
}
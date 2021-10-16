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
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * @author shicai.xsc 2019/1/23 下午5:32
 * @since 5.0.0.0
 */
public class TypeUtils {

    public static class MathLevel {

        public static int OTHER = 100;
        public int level = OTHER;
        public DataType type;

        public boolean isOther() {
            return level == OTHER;
        }
    }

    @Deprecated
    public static MathLevel getMathLevel(DataType argType) {
        MathLevel ml = new MathLevel();
        if (DataTypeUtil.equalsSemantically(DataTypes.BytesType, argType)) {
            ml.level = 0;
            ml.type = DataTypes.BytesType;
        } else if (DataTypeUtil.equalsSemantically(DataTypes.FloatType, argType)
            || DataTypeUtil.equalsSemantically(DataTypes.DoubleType, argType)
            || DataTypeUtil.equalsSemantically(DataTypes.DecimalType, argType)) {
            ml.level = 1;
            ml.type = DataTypes.DecimalType;
        } else if (DataTypeUtil.equalsSemantically(DataTypes.ULongType, argType)) {
            ml.level = 2;
            ml.type = DataTypes.ULongType;
        } else if (DataTypeUtil.equalsSemantically(DataTypes.ULongType, argType)) {
            ml.level = 3;
            ml.type = DataTypes.ULongType;
        } else if (DataTypeUtil.isUnderIntType(argType)) {
            ml.level = 5;
            ml.type = DataTypes.IntegerType;
        } else if (DataTypeUtil.isUnderLongType(argType) || DataTypeUtil.equalsSemantically(DataTypes.BitType, argType)
            || DataTypeUtil.equalsSemantically(DataTypes.YearType, argType)) {
            ml.level = 4;
            ml.type = DataTypes.LongType;
        } else {
            ml.level = MathLevel.OTHER;
            ml.type = argType;
        }

        return ml;
    }
}

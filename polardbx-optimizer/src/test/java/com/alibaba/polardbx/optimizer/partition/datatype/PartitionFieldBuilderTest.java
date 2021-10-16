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

package com.alibaba.polardbx.optimizer.partition.datatype;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class PartitionFieldBuilderTest {
    @Test
    public void testAllSupported() {
        ImmutableList<DataType> supported = ImmutableList.of(
            DataTypes.TinyIntType,
            DataTypes.UTinyIntType,
            DataTypes.SmallIntType,
            DataTypes.USmallIntType,
            DataTypes.MediumIntType,
            DataTypes.UMediumIntType,
            DataTypes.IntegerType,
            DataTypes.UIntegerType,
            DataTypes.LongType,
            DataTypes.ULongType,
            DataTypes.VarcharType,
            DataTypes.CharType,
            DataTypes.DatetimeType,
            DataTypes.TimestampType,
            DataTypes.DateType
        );

        supported.stream().map(PartitionFieldBuilder::createField);
    }
}

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

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.DecimalField;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

public class DecimalPartitionField extends AbstractNumericPartitionField {

    public DecimalPartitionField(DataType<?> dataType) {
        field = new DecimalField(dataType);
    }

    @Override
    public int compareTo(@NotNull PartitionField o) {
        Preconditions.checkArgument(this.mysqlStandardFieldType() == o.mysqlStandardFieldType());
        Decimal a = decimalValue();
        Decimal b = o.decimalValue();
        return a.compareTo(b);
    }

    @Override
    public Decimal decimalValue() {
        return ((DecimalField) field).decimalValue();
    }

    @Override
    public double doubleValue() {
        return ((DecimalField) field).doubleValue();
    }

    @Override
    public Decimal decimalValue(SessionProperties sessionProperties) {
        return ((DecimalField) field).decimalValue(sessionProperties);
    }

    @Override
    public double doubleValue(SessionProperties sessionProperties) {
        return ((DecimalField) field).doubleValue(sessionProperties);
    }
}

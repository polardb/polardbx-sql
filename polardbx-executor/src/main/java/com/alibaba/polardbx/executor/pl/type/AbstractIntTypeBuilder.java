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

package com.alibaba.polardbx.executor.pl.type;

import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;

public abstract class AbstractIntTypeBuilder extends AbstractBasicSqlTypeBuilder {
    @Override
    int getPrecision(SQLDataType dataType) {
        return !typeHasArgs(dataType) ? getDefaultPrecision() : getFirstArgAsPrecision(dataType);
    }

    abstract int getDefaultPrecision();

    @Override
    SqlTypeName getTypeName(SQLDataType dataType) {
        if (!(dataType instanceof SQLDataTypeImpl)) {
            throw new RuntimeException(String.format("dataType: %s parse failed!", dataType.toString()));
        }
        if (((SQLDataTypeImpl) dataType).isUnsigned()) {
            return unsignedType();
        } else {
            return signedType();
        }
    }

    abstract SqlTypeName signedType();

    abstract SqlTypeName unsignedType();
}

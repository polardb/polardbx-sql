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

package com.alibaba.polardbx.optimizer.core;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Created by lingce.ldm on 2017/2/6.
 */
public class TddlRelDataTypeSystemImpl extends RelDataTypeSystemImpl {

    private static RelDataTypeSystem instance = new TddlRelDataTypeSystemImpl();

    public static RelDataTypeSystem getInstance() {
        return instance;
    }

    private TddlRelDataTypeSystemImpl() {
    }

    /**
     * TDDL 对于字段名是大小写不敏感的
     */
    @Override
    public boolean isSchemaCaseSensitive() {
        return false;
    }

    @Override
    public RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        RelDataType sumType;
        switch (argumentType.getSqlTypeName()) {
        case DECIMAL:
            return argumentType;
        case VARCHAR:
        case CHAR:
        case BINARY:
        case VARBINARY:
        case BLOB:
        case ENUM:
        case JSON:
        case FLOAT:
        case DOUBLE:
        case USER_SYMBOL:
            sumType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
            break;
        default:
            sumType = typeFactory.createSqlType(SqlTypeName.DECIMAL);
            break;
        }
        return sumType;
    }

    @Override
    public RelDataType deriveAvgAggType(RelDataTypeFactory typeFactory, RelDataType argumentType) {
        RelDataType inferredType;
        int precision = argumentType.getPrecision();
        int scale = argumentType.getScale();
        final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();

        final int maxNumericPrecision = typeSystem.getMaxNumericPrecision();
        int dout = Math.min(precision, maxNumericPrecision);

        scale = Math.max(6, scale + precision + 1);
        scale = Math.min(scale, maxNumericPrecision - dout);
        scale = Math.min(scale, typeSystem.getMaxNumericScale());

        precision = dout + scale;

        assert scale <= typeSystem.getMaxNumericScale();
        precision = Math.min(precision, typeSystem.getMaxNumericPrecision());
        assert precision > 0;

        switch (argumentType.getSqlTypeName()) {
        case VARCHAR:
        case CHAR:
        case BINARY:
        case VARBINARY:
        case BLOB:
        case ENUM:
        case JSON:
        case FLOAT:
        case DOUBLE:
            inferredType = typeFactory.createSqlType(SqlTypeName.DOUBLE, precision, scale);
            break;
        default:
            inferredType = typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        }
        return typeFactory.createTypeWithNullability(inferredType, true);
    }

    @Override
    public int getMaxNumericScale() {
        return 30;
    }

    @Override
    public int getMaxNumericPrecision() {
        return 65;
    }
}

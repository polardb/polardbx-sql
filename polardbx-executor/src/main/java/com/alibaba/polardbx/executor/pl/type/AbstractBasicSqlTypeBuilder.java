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
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Optional;

public abstract class AbstractBasicSqlTypeBuilder implements BasicSqlTypeBuilder {
    @Override
    public RelDataType createBasicSqlType(RelDataTypeSystem typeSystem,
                                          SQLDataType dataType) {
        int scale = getScale(dataType);
        int precision = getPrecision(dataType);
        SqlTypeName name = getTypeName(dataType);
        return new BasicSqlType(typeSystem, name, precision, scale);
    }

    int getScale(SQLDataType dataType) {
        return 0;
    }

    int getPrecision(SQLDataType dataType) {
        return RelDataType.PRECISION_NOT_SPECIFIED;
    }

    abstract SqlTypeName getTypeName(SQLDataType dataType);

    /*** helper function ***/
    protected int getFirstArgAsPrecision(SQLDataType dataType) {
        return Optional.ofNullable(dataType.getArguments()).map(t -> t.get(0)).filter(t -> t instanceof SQLIntegerExpr)
            .map(t -> (Integer) (((SQLIntegerExpr) t).getValue())).orElse(0);
    }

    protected int getFirstArgAsScale(SQLDataType dataType) {
        return Optional.ofNullable(dataType.getArguments()).map(t -> t.get(0)).filter(t -> t instanceof SQLIntegerExpr)
            .map(t -> (Integer) (((SQLIntegerExpr) t).getValue())).orElse(RelDataType.PRECISION_NOT_SPECIFIED);
    }

    protected int getSecondArgAsScale(SQLDataType dataType) {
        return Optional.ofNullable(dataType.getArguments()).map(t -> t.get(1)).filter(t -> t instanceof SQLIntegerExpr)
            .map(t -> (Integer) (((SQLIntegerExpr) t).getValue())).orElse(-1);
    }

    protected boolean typeHasArgs(SQLDataType dataType) {
        return dataType.getArguments() != null && dataType.getArguments().size() > 0;
    }
}

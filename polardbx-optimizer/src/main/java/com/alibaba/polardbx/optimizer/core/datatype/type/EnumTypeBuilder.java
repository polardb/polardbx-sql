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

package com.alibaba.polardbx.optimizer.core.datatype.type;

import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.EnumSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

public class EnumTypeBuilder implements BasicSqlTypeBuilder {
    public static EnumTypeBuilder INSTANCE = new EnumTypeBuilder();

    private EnumTypeBuilder() {
    }

    @Override
    public RelDataType createBasicSqlType(RelDataTypeSystem typeSystem, SQLDataType dataType) {
        List<String> list = new ArrayList();
        for (SQLExpr expr : dataType.getArguments()) {
            assert expr instanceof SQLCharExpr;
            list.add(((SQLCharExpr) expr).getText());
        }
        return new EnumSqlType(typeSystem, SqlTypeName.ENUM, list, null, null);
    }
}

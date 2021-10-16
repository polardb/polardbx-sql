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

package com.alibaba.polardbx.optimizer.view;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

public class InformationSchemaParameters extends VirtualView {

    public InformationSchemaParameters(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.PARAMETERS);
    }

    public InformationSchemaParameters(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("SPECIFIC_CATALOG", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SPECIFIC_SCHEMA", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SPECIFIC_NAME", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ORDINAL_POSITION", 3, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("PARAMETER_MODE", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PARAMETER_NAME", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DATA_TYPE", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("CHARACTER_MAXIMUM_LENGTH", 7, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns
            .add(new RelDataTypeFieldImpl("CHARACTER_OCTET_LENGTH", 8, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("NUMERIC_PRECISION", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("NUMERIC_SCALE", 10, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("DATETIME_PRECISION", 11, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("CHARACTER_SET_NAME", 12, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("COLLATION_NAME", 13, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DTD_IDENTIFIER", 14, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ROUTINE_TYPE", 15, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }
}

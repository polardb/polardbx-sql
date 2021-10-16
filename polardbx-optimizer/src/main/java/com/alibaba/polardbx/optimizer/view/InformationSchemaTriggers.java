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

/**
 * @author dylan
 */
public class InformationSchemaTriggers extends VirtualView {

    public InformationSchemaTriggers(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.TRIGGERS);
    }

    public InformationSchemaTriggers(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        columns.add(new RelDataTypeFieldImpl("TRIGGER_CATALOG", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TRIGGER_SCHEMA", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TRIGGER_NAME", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("EVENT_MANIPULATION", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns
            .add(new RelDataTypeFieldImpl("EVENT_OBJECT_CATALOG", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("EVENT_OBJECT_SCHEMA", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("EVENT_OBJECT_TABLE", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ACTION_ORDER", 7, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("ACTION_CONDITION", 8, typeFactory.createSqlType(SqlTypeName.BINARY)));
        columns.add(new RelDataTypeFieldImpl("ACTION_STATEMENT", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ACTION_ORIENTATION", 10, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ACTION_TIMING", 11, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("ACTION_REFERENCE_OLD_TABLE", 12, typeFactory.createSqlType(SqlTypeName.BINARY)));
        columns.add(
            new RelDataTypeFieldImpl("ACTION_REFERENCE_NEW_TABLE", 13, typeFactory.createSqlType(SqlTypeName.BINARY)));
        columns.add(
            new RelDataTypeFieldImpl("ACTION_REFERENCE_OLD_ROW", 14, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("ACTION_REFERENCE_NEW_ROW", 15, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CREATED", 16, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(new RelDataTypeFieldImpl("SQL_MODE", 17, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DEFINER", 18, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns
            .add(new RelDataTypeFieldImpl("CHARACTER_SET_CLIENT", 19, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns
            .add(new RelDataTypeFieldImpl("COLLATION_CONNECTION", 20, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DATABASE_COLLATION", 21, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }
}



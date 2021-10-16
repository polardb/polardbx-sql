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
public class InformationSchemaColumnStatistics extends VirtualView {

    public InformationSchemaColumnStatistics(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.COLUMN_STATISTICS);
    }

    public InformationSchemaColumnStatistics(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();

        columns.add(new RelDataTypeFieldImpl("SCHEMA_NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLE_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("COLUMN_NAME", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("HISTOGRAM", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // SCHEMA_NAME && TABLE_NAME
        return i == getSchemaNameIndex() || i == getTableNameIndex();
    }

    public int getSchemaNameIndex() {
        return 0;
    }

    public int getTableNameIndex() {
        return 1;
    }
}




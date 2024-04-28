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

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTableDetail extends VirtualView {

    protected InformationSchemaTableDetail(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.TABLE_DETAIL);
    }

    public InformationSchemaTableDetail(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        int i = 0;
        columns.add(new RelDataTypeFieldImpl("TABLE_SCHEMA", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLE_GROUP_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TABLE_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("INDEX_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PHYSICAL_TABLE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(
            new RelDataTypeFieldImpl("PARTITION_SEQ", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("PARTITION_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(
            new RelDataTypeFieldImpl("SUBPARTITION_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("SUBPARTITION_TEMPLATE_NAME", i++,
                typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(
            new RelDataTypeFieldImpl("TABLE_ROWS", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(
            new RelDataTypeFieldImpl("DATA_LENGTH", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(
            new RelDataTypeFieldImpl("INDEX_LENGTH", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("BOUND_VALUE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SUB_BOUND_VALUE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PERCENT", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("STORAGE_INST_ID", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("GROUP_NAME", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        // QPS statistics
        columns.add(new RelDataTypeFieldImpl("ROWS_READ", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(
            new RelDataTypeFieldImpl("ROWS_INSERTED", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(
            new RelDataTypeFieldImpl("ROWS_UPDATED", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(
            new RelDataTypeFieldImpl("ROWS_DELETED", i++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));

        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // SCHEMA_NAME && LOGICAL_TABLE
        return i == getTableSchemaIndex() || i == getTableNameIndex();
    }

    private static final List<Integer> INDEXABLE_COLUMNS;

    static {
        INDEXABLE_COLUMNS = Lists.newArrayList(getTableSchemaIndex(), getTableNameIndex());
    }

    @Override
    List<Integer> indexableColumnList() {
        return INDEXABLE_COLUMNS;
    }

    static public int getTableSchemaIndex() {
        return 0;
    }

    static public int getTableNameIndex() {
        return 2;
    }
}

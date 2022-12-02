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

public class InformationSchemaArchive extends VirtualView {

    public InformationSchemaArchive(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.ARCHIVE);
    }

    public InformationSchemaArchive(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        int index = 0;
        // archive-source mapping info
        columns.add(new RelDataTypeFieldImpl("ARCHIVE_TABLE_SCHEMA", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ARCHIVE_TABLE_NAME", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LOCAL_PARTITION_TABLE_SCHEMA", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LOCAL_PARTITION_TABLE_NAME", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        // local partition info
        columns.add(new RelDataTypeFieldImpl("LOCAL_PARTITION_INTERVAL_COUNT", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("LOCAL_PARTITION_INTERVAL_UNIT", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LOCAL_PARTITION_EXPIRE_AFTER", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("LOCAL_PARTITION_PREALLOCATE", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("LOCAL_PARTITION_PIVOT_DATE", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        // schedule info
        columns.add(new RelDataTypeFieldImpl("SCHEDULE_ID", index++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SCHEDULE_STATUS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SCHEDULE_EXPR", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SCHEDULE_COMMENT", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SCHEDULE_TIME_ZONE", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LAST_FIRE_TIME", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("NEXT_FIRE_TIME", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("NEXT_EVENT", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        // archive execution info
        columns.add(new RelDataTypeFieldImpl("LAST_SUCCESS_ARCHIVE_TIME", index++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(new RelDataTypeFieldImpl("ARCHIVE_STATUS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ARCHIVE_PROGRESS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ARCHIVE_JOB_PROGRESS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ARCHIVE_CURRENT_TASK", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ARCHIVE_CURRENT_TASK_PROGRESS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        return typeFactory.createStructType(columns);
    }

    @Override
    boolean indexableColumn(int i) {
        // TABLE_SCHEMA && TABLE_NAME
        return i == getTableSchemaIndex() || i == getTableNameIndex();
    }

    public int getTableSchemaIndex() {
        return 0;
    }

    public int getTableNameIndex() {
        return 1;
    }
}

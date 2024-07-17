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

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaRebalanceProgress extends VirtualView {

    protected InformationSchemaRebalanceProgress(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.REBALANCE_PROGRESS);
    }

    public InformationSchemaRebalanceProgress(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        int i = 0;
        columns.add(new RelDataTypeFieldImpl("JOB_ID", i++, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TABLE_SCHEMA", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("STAGE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("STATE", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PROGRESS", i++, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("TOTAL_TASK", i++, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(
            new RelDataTypeFieldImpl("FINISHED_TASK", i++, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(
            new RelDataTypeFieldImpl("RUNNING_TASK", i++, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(
            new RelDataTypeFieldImpl("NOTSTARTED_TASK", i++, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("FAILED_TASK", i++, typeFactory.createSqlType(SqlTypeName.INTEGER)));
        columns.add(new RelDataTypeFieldImpl("INFO", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("START_TIME", i++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(
            new RelDataTypeFieldImpl("LAST_UPDATE_TIME", i++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        columns.add(new RelDataTypeFieldImpl("DDL_STMT", i++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        return typeFactory.createStructType(columns);
    }
}

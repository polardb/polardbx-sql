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
 * @author yaozhili
 */
public class InformationSchemaReplicaStat extends VirtualView {
    public InformationSchemaReplicaStat(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.REPLICA_STAT);
    }

    public InformationSchemaReplicaStat(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        int columnIndex = 0;
        columns.add(
            new RelDataTypeFieldImpl("TASK_ID", columnIndex++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("TASK_TYPE", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CHANNEL", columnIndex++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("SUB_CHANNEL", columnIndex++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("IN_EPS", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("OUT_RPS", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("IN_BPS", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("OUT_BPS", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("OUT_INSERT_RPS", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("OUT_UPDATE_RPS", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("OUT_DELETE_RPS", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("APPLY_COUNT", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("RECEIVE_DELAY", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("PROCESS_DELAY", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("MERGE_BATCH_SIZE", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(
            new RelDataTypeFieldImpl("RT", columnIndex++, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("SKIP_COUNTER", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("SKIP_EXCEPTION_COUNTER", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("PERSIST_MSG_COUNTER", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("MSG_CACHE_SIZE", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("CPU_USE_RATIO", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("MEM_USE_RATIO", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
        columns.add(new RelDataTypeFieldImpl("FULL_GC_COUNT", columnIndex++,
            typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
        columns.add(
            new RelDataTypeFieldImpl("WORKER_IP", columnIndex++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(
            new RelDataTypeFieldImpl("UPDATE_TIME", columnIndex++, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
        return typeFactory.createStructType(columns);
    }
}

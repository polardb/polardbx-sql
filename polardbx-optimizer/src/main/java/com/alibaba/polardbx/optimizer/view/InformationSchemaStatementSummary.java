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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedList;
import java.util.List;

/**
 * @author busu
 * date: 2021/11/15 8:23 下午
 */
public class InformationSchemaStatementSummary extends VirtualView {

    public InformationSchemaStatementSummary(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.STATEMENTS_SUMMARY);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new LinkedList<>();
        columns.add(new RelDataTypeFieldImpl("BEGIN_TIME", 0, typeFactory.createSqlType(SqlTypeName.DATETIME)));
        columns.add(new RelDataTypeFieldImpl("SCHEMA", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SQL_TYPE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TEMPLATE_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PLAN_HASH", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SQL_TEMPLATE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("COUNT", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("ERROR_COUNT", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("SUM_RESPONSE_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("AVG_RESPONSE_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("MAX_RESPONSE_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns.add(new RelDataTypeFieldImpl("SUM_AFFECTED_ROWS", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("AVG_AFFECTED_ROWS", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("MAX_AFFECTED_ROWS", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns
            .add(new RelDataTypeFieldImpl("SUM_TRANSACTION_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns
            .add(new RelDataTypeFieldImpl("AVG_TRANSACTION_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns
            .add(new RelDataTypeFieldImpl("MAX_TRANSACTION_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns
            .add(new RelDataTypeFieldImpl("SUM_BUILD_PLAN_CPU_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns
            .add(new RelDataTypeFieldImpl("AVG_BUILD_PLAN_CPU_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns
            .add(new RelDataTypeFieldImpl("MAX_BUILD_PLAN_CPU_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns
            .add(new RelDataTypeFieldImpl("SUM_EXEC_PLAN_CPU_TIME_MS", 0,
                typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns
            .add(new RelDataTypeFieldImpl("AVG_EXEC_PLAN_CPU_TIME_MS", 0,
                typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns
            .add(new RelDataTypeFieldImpl("MAX_EXEC_PLAN_CPU_TIME_MS", 0,
                typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("SUM_PHYSICAL_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("AVG_PHYSICAL_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("MAX_PHYSICAL_TIME_MS", 0, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns
            .add(new RelDataTypeFieldImpl("SUM_PHYSICAL_EXEC_COUNT", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("AVG_PHYSICAL_EXEC_COUNT", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("MAX_PHYSICAL_EXEC_COUNT", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns
            .add(new RelDataTypeFieldImpl("SUM_PHYSICAL_FETCH_ROWS", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("AVG_PHYSICAL_FETCH_ROWS", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns
            .add(new RelDataTypeFieldImpl("MAX_PHYSICAL_FETCH_ROWS", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("FIRST_SEEN", 0, typeFactory.createSqlType(SqlTypeName.DATETIME)));
        columns.add(new RelDataTypeFieldImpl("LAST_SEEN", 0, typeFactory.createSqlType(SqlTypeName.DATETIME)));
        columns.add(new RelDataTypeFieldImpl("SQL_SAMPLE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PREV_TEMPLATE_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("PREV_SQL_TEMPLATE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SAMPLE_TRACE_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("WORKLOAD_TYPE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("EXECUTE_MODE", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        return typeFactory.createStructType(columns);
    }

}

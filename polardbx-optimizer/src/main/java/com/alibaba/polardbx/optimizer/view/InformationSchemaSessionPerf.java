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
 * @version 1.0
 */
public class InformationSchemaSessionPerf extends VirtualView {

    public InformationSchemaSessionPerf(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.SESSION_PERF);
    }

    public InformationSchemaSessionPerf(RelInput input) {
        super(input);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        columns.add(new RelDataTypeFieldImpl("CN", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DN", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TCP", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SESSION", 3, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("STATUS", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(new RelDataTypeFieldImpl("PLAN_COUNT", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("QUERY_COUNT", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("UPDATE_COUNT", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TSO_COUNT", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("LIVE_TIME", 9, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(
            new RelDataTypeFieldImpl("TIME_SINCE_LAST_RECV", 10, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns.add(new RelDataTypeFieldImpl("CHARSET_CLIENT", 11, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("CHARSET_RESULT", 12, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TIMEZONE", 13, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("ISOLATION", 14, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("AUTO_COMMIT", 15, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
        columns.add(new RelDataTypeFieldImpl("VARIABLES_CHANGED", 16, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("LAST_DB", 17, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(
            new RelDataTypeFieldImpl("QUEUED_REQUEST_DEPTH", 18, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("TRACE_ID", 19, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("SQL", 20, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("EXTRA", 21, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TYPE", 22, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("REQUEST_STATUS", 23, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("FETCH_COUNT", 24, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TOKEN_COUNT", 25, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("TIME_SINCE_REQUEST", 26, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl(
            "DATA_PKT_RESPONSE_TIME", 27, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("RESPONSE_TIME", 28, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("FINISH_TIME", 29, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("TOKEN_DONE_COUNT", 30, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "ACTIVE_OFFER_TOKEN_COUNT", 31, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("START_TIME", 32, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("RESULT_CHUNK", 33, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
        columns.add(new RelDataTypeFieldImpl("RETRANSMIT", 34, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
        columns.add(new RelDataTypeFieldImpl("USE_CACHE", 35, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));

        return typeFactory.createStructType(columns);
    }
}

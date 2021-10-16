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
public class InformationSchemaDnPerf extends VirtualView {

    public InformationSchemaDnPerf(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.DN_PERF);
    }

    public InformationSchemaDnPerf(RelInput relInput) {
        super(relInput);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        columns.add(new RelDataTypeFieldImpl("CN", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DN", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(new RelDataTypeFieldImpl("SEND_MSG_COUNT", 2, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SEND_FLUSH_COUNT", 3, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SEND_SIZE", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("RECV_MSG_COUNT", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("RECV_NET_COUNT", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("RECV_SIZE", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("SESSION_CREATE_COUNT", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SESSION_DROP_COUNT", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "SESSION_CREATE_SUCCESS_COUNT", 10, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "SESSION_CREATE_FAIL_COUNT", 11, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("DN_CONCURRENT_COUNT", 12, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "WAIT_CONNECTION_COUNT", 13, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("SEND_MSG_RATE", 14, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("SEND_FLUSH_RATE", 15, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("SEND_RATE", 16, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("RECV_MSG_RATE", 17, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("RECV_NET_RATE", 18, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("RECV_RATE", 19, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("SESSION_CREATE_RATE", 20, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("SESSION_DROP_RATE", 21, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl(
            "SESSION_CREATE_SUCCESS_RATE", 22, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl(
            "SESSION_CREATE_FAIL_RATE", 23, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("TIME_DELTA", 24, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns.add(new RelDataTypeFieldImpl("TCP_COUNT", 25, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("AGING_TCP_COUNT", 26, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SESSION_COUNT", 27, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SESSION_IDLE_COUNT", 28, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("REF_COUNT", 29, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        return typeFactory.createStructType(columns);
    }
}

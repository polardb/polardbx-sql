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
public class InformationSchemaTcpPerf extends VirtualView {

    public InformationSchemaTcpPerf(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet, VirtualViewType.TCP_PERF);
    }

    public InformationSchemaTcpPerf(RelInput input) {
        super(input);
    }

    @Override
    protected RelDataType deriveRowType() {
        final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
        List<RelDataTypeFieldImpl> columns = new ArrayList<>();

        columns.add(new RelDataTypeFieldImpl("CN", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("TCP", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("DN", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(new RelDataTypeFieldImpl("SEND_MSG_COUNT", 3, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SEND_FLUSH_COUNT", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SEND_SIZE", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("RECV_MSG_COUNT", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("RECV_NET_COUNT", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("RECV_SIZE", 8, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("SESSION_CREATE_COUNT", 9, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("SESSION_DROP_COUNT", 10, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "SESSION_CREATE_SUCCESS_COUNT", 11, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "SESSION_CREATE_FAIL_COUNT", 12, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("DN_CONCURRENT_COUNT", 13, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("SESSION_COUNT", 14, typeFactory.createSqlType(SqlTypeName.BIGINT)));

        columns.add(new RelDataTypeFieldImpl("CLIENT_STATE", 15, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl(
            "TIME_SINCE_LAST_RECV", 16, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("LIVE_TIME", 17, typeFactory.createSqlType(SqlTypeName.DOUBLE)));
        columns.add(new RelDataTypeFieldImpl("FATAL_ERROR", 18, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
        columns.add(new RelDataTypeFieldImpl("AUTH_ID", 19, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "TIME_SINCE_VARIABLES_REFRESH", 20, typeFactory.createSqlType(SqlTypeName.DOUBLE)));

        columns.add(new RelDataTypeFieldImpl("TCP_STATE", 21, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

        columns.add(new RelDataTypeFieldImpl(
            "SOCKET_SEND_BUFFER_SIZE", 22, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "SOCKET_RECV_BUFFER_SIZE", 23, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("READ_DIRECT_BUFFERS", 24, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("READ_HEAP_BUFFERS", 25, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl(
            "WRITE_DIRECT_BUFFERS", 26, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("WRITE_HEAP_BUFFERS", 27, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        columns.add(new RelDataTypeFieldImpl("REACTOR_REGISTERED", 28, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
        columns.add(new RelDataTypeFieldImpl("SOCKET_CLOSED", 29, typeFactory.createSqlType(SqlTypeName.BOOLEAN)));

        return typeFactory.createStructType(columns);
    }
}

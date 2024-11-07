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

package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author yaozhili
 */
public interface ICciChecker {
    default void check(ExecutionContext baseEc) throws Throwable {
        check(baseEc, null);
    }

    default void check(ExecutionContext baseEc, Runnable recoverChangedConfigs) throws Throwable {
        throw new UnsupportedOperationException();
    }

    default void check(ExecutionContext baseEc, long tsoV1, long tsoV2, long innodbTso) throws Throwable {
        throw new UnsupportedOperationException();
    }

    /**
     * @param reports [OUT] check reports returned
     * @return true if anything is ok (reports may be empty),
     * or false if inconsistency detected (inconsistency details are in reports)
     */
    boolean getCheckReports(Collection<String> reports);

    static long getTableId(String schemaName, String indexName) throws SQLException {
        return DynamicColumnarManager.getInstance().getTableId(0, schemaName, indexName);
    }

    static List<FilesRecordSimplifiedWithChecksum> getFilesRecords(long tso, long tableId, String schemaName)
        throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection()) {
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);
            return filesAccessor.querySnapshotWithChecksumByTsoAndTableId(tso, schemaName, String.valueOf(tableId));
        }
    }

    static void setBasicHint(ExecutionContext ec, StringBuilder sb) {
        long parallelism;
        if ((parallelism = ec.getParamManager().getInt(ConnectionParams.MPP_PARALLELISM)) > 0) {
            sb.append(" MPP_PARALLELISM=")
                .append(parallelism)
                .append(" ");
        }
        if ((parallelism = ec.getParamManager().getInt(ConnectionParams.PARALLELISM)) > 0) {
            sb.append(" PARALLELISM=")
                .append(parallelism)
                .append(" ");
        }
        boolean enableMpp = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MPP);
        if (enableMpp) {
            sb.append(" ENABLE_MPP=true");
        }
        boolean enableMasterMpp = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_MASTER_MPP);
        if (enableMasterMpp) {
            sb.append(" ENABLE_MASTER_MPP=true");
        }
        sb.append(" ENABLE_ACCURATE_REL_TYPE_TO_DATA_TYPE=true");
    }

    /**
     * Don't forget to close it.
     */
    static IInnerConnection startDaemonTransaction(IInnerConnectionManager mgr, String schema, String table)
        throws SQLException {
        IInnerConnection connection = mgr.getConnection(schema);
        connection.setTrxPolicy(ITransactionPolicy.TSO);
        connection.setAutoCommit(false);
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("show topology from " + table);
            List<Pair<String, String>> topologies = new ArrayList<>();
            while (rs.next()) {
                topologies.add(new Pair<>(rs.getString("GROUP_NAME"), rs.getString("TABLE_NAME")));
            }
            // For each group, send a query.
            for (Pair<String, String> topology : topologies) {
                stmt.executeQuery(String.format("/*+TDDL:node(%s)*/ select 1 from %s limit 1", topology.getKey(),
                    topology.getValue()));
            }
        }
        return connection;
    }
}

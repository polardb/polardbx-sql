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

package com.alibaba.polardbx.optimizer.config.server;

import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

/**
 * interfaces for the Optimizer/Execoutr of TDataSource to notify CobarServer to
 * init other APP_NAME
 *
 * @author chenghui.lch 2018年5月22日 下午3:22:32
 * @since 5.0.0
 */
public interface IServerConfigManager {

    Object getAndInitDataSourceByDbName(String dbName);

    /**
     * Find the schema and group name according to a 64-bit unique id.
     * <p>
     * Currently, this method is used by distributed transaction module to locate the primary group by
     * a limited-length identifier in XID.
     *
     * @param uniqueId group unique id
     * @return a pair of string containing schema name and group name
     */
    Pair<String, String> findGroupByUniqueId(long uniqueId);

    /**
     * Perform a DDL job
     *
     * @param job a DDL job
     * @return warnings
     */
    default List<ExecutionContext.ErrorMessage> performAsyncDDLJob(Job job, String schemaName, Object jobRequest) {
        throw new UnsupportedOperationException("TODO");
    }

    default void executeBackgroundSql(String sql, String schema, InternalTimeZone timeZone) {
        throw new UnsupportedOperationException("not supported");
    }

    default List<Map<String, Object>> executeQuerySql(String sql, String schema, InternalTimeZone timeZone) {
        throw new UnsupportedOperationException("not supported");
    }

    /**
     * Restore a DDL from GMS then execute it
     */
    DdlContext restoreDDL(String schemaName, Long jobId);

    void remoteExecuteDdlTask(String schemaName, Long jobId, Long taskId);

    long submitRebalanceDDL(String schemaName, String sql);

    long submitSubDDL(String schemaName, DdlContext parentDdlContext, long parentJobId, long parentTaskId,
                      boolean forRollback,
                      String sql, ParamManager paramManager);

    static long getGroupUniqueId(String schema, String group) {
        // normalization is included in this method
        return FnvHash.hashCode64(schema, group);
    }

}

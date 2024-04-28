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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.cdc.CdcConfigAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSetCdcGlobal;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.util.List;
import java.util.Properties;

/**
 * @author yudong
 * @since 2023/6/20 14:37
 **/
public class LogicalSetCdcGlobalHandler extends HandlerCommon {

    private static final Logger cdcLogger = SQLRecorderLogger.cdcLogger;

    public LogicalSetCdcGlobalHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlSetCdcGlobal sqlSetCdcGlobal = (SqlSetCdcGlobal) ((LogicalDal) logicalPlan).getNativeSqlNode();
        List<Pair<SqlNode, SqlNode>> variableAssignmentList = sqlSetCdcGlobal.getVariableAssignmentList();
        SqlNode with = sqlSetCdcGlobal.getWith();
        String configKeyPrefix = with == null ? "" : with.toString().replace("'", "") + ":";

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            CdcConfigAccessor accessor = new CdcConfigAccessor();
            accessor.setConnection(metaDbConn);
            Properties props = new Properties();
            for (Pair<SqlNode, SqlNode> pair : variableAssignmentList) {
                String key = pair.getKey().toString();
                key = key.replace("'", "");
                String configKey = configKeyPrefix + key;
                String configValue = pair.getValue().toString();
                props.setProperty(configKey, configValue);
            }
            accessor.updateInstConfigValue(props);
        } catch (Exception e) {
            cdcLogger.error("set cdc global error", e);
            throw new TddlNestableRuntimeException();
        }

        return new AffectRowCursor(0);
    }
}

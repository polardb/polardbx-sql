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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowReplicaCheckDiff;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author yudong
 * @since 2023/11/9 10:41
 **/
public class LogicalShowReplicaCheckDiffHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowReplicaCheckDiffHandler.class);

    public LogicalShowReplicaCheckDiffHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        SqlShowReplicaCheckDiff sqlShowReplicaCheckDiff =
            (SqlShowReplicaCheckDiff) ((LogicalDal) logicalPlan).getNativeSqlNode();
        String dbName = sqlShowReplicaCheckDiff.getDbName().toString();
        if (StringUtils.isEmpty(dbName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, "database cannot be empty!");
        }
        String tbName = null;
        String sql;
        if (sqlShowReplicaCheckDiff.getTableName() != null) {
            tbName = sqlShowReplicaCheckDiff.getTableName().toString();
            sql = String.format("SELECT * FROM " + GmsSystemTables.RPL_FULL_VALID_DIFF_TABLE
                + " WHERE `dst_logical_db` = '%s' AND `dst_logical_table` = '%s'", dbName, tbName);
        } else {
            sql = String.format("SELECT * FROM " + GmsSystemTables.RPL_FULL_VALID_DIFF_TABLE
                + " WHERE `dst_logical_db` = '%s'", dbName);
        }

        final ArrayResultCursor result = new ArrayResultCursor("CHECK REPLICA TABLE SHOW DIFF");
        result.addColumn("DATABASE", DataTypes.StringType);
        result.addColumn("TABLE", DataTypes.StringType);
        result.addColumn("ERROR_TYPE", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("SRC_KEY_NAME", DataTypes.StringType);
        result.addColumn("SRC_KEY_VAL", DataTypes.StringType);
        result.addColumn("DST_KEY_NAME", DataTypes.StringType);
        result.addColumn("DST_KEY_VAL", DataTypes.StringType);
        result.initMeta();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            PreparedStatement statement = metaDbConn.prepareStatement(sql);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                result.addRow(new Object[] {
                    rs.getString("dst_logical_db"),
                    rs.getString("dst_logical_table"),
                    rs.getString("error_type"),
                    rs.getString("status"),
                    rs.getString("src_key_name"),
                    rs.getString("src_key_val"),
                    rs.getString("dst_key_name"),
                    rs.getString("dst_key_val")
                });
            }
            return result;
        } catch (SQLException ex) {
            logger.error("get replica diff failed!", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_REPLICATION_RESULT, ex, ex.getMessage());
        }
    }
}

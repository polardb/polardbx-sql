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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureStatusRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlShowFunctionStatus;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

public class LogicalShowFunctionStatusMyHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowFunctionStatusMyHandler.class);

    public LogicalShowFunctionStatusMyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow showNode = (LogicalShow) logicalPlan;
        SqlShowFunctionStatus show = (SqlShowFunctionStatus) showNode.getNativeSqlNode();
        String like = "%";
        if (show.like instanceof SqlCharStringLiteral) {
            like = ((SqlCharStringLiteral) show.like).getNlsString().getValue();
        }
        try (Connection connection = MetaDbUtil.getConnection()) {
            ArrayResultCursor result = new ArrayResultCursor("ROUTINES");
            result.addColumn("Db", DataTypes.StringType);
            result.addColumn("Name", DataTypes.StringType);
            result.addColumn("Type", DataTypes.StringType);
            result.addColumn("Definer", DataTypes.StringType);
            result.addColumn("Modified", DataTypes.DatetimeType);
            result.addColumn("Created", DataTypes.DatetimeType);
            result.addColumn("Security_type", DataTypes.StringType);
            result.addColumn("Comment", DataTypes.StringType);
            result.addColumn("character_set_client", DataTypes.StringType);
            result.addColumn("collation_connection", DataTypes.StringType);
            result.addColumn("Database Collation", DataTypes.StringType);
            result.initMeta();

            FunctionAccessor accessor = new FunctionAccessor();
            accessor.setConnection(connection);
            List<ProcedureStatusRecord> records = accessor.getFunctionStatus(like);
            for (ProcedureStatusRecord record : records) {
                String db = record.schema;
                String name = record.name;
                String type = record.type;
                String definer = record.definer;
                Timestamp modified = record.lastAltered;
                Timestamp created = record.created;
                String securityType = record.securityType;
                String comment = record.routineComment;
                String characterSetClient = record.characterSetClient;
                String collationConnection = record.collationConnection;
                String databaseCollation = record.databaseCollation;

                result.addRow(new Object[] {
                    db, name, type, definer, modified, created, securityType, comment,
                    characterSetClient, collationConnection, databaseCollation});
            }

            return result;
        } catch (SQLException ex) {
            logger.error("show function status failed", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_UDF_EXECUTE, ex);
        }
    }
}

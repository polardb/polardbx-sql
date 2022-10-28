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

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.mdl.MdlTicket;
import com.alibaba.polardbx.executor.mdl.context.MdlContextStamped;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowMetadataLock;

import java.util.Map;

/**
 * @version 1.0
 */
public class ShowMetadataLockHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(ShowMetadataLockHandler.class);

    public ShowMetadataLockHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor resultCursor = buildResultCursor();

        SqlShowMetadataLock showMetadataLock = (SqlShowMetadataLock) ((LogicalDal) logicalPlan).getNativeSqlNode();
        String schemaName =
            null == showMetadataLock.getSchema() ? null : ((SqlIdentifier) showMetadataLock.getSchema()).names.get(0);
        String tableName =
            null == showMetadataLock.getSchema() || ((SqlIdentifier) showMetadataLock.getSchema()).names.size() != 2 ?
                null : ((SqlIdentifier) showMetadataLock.getSchema()).names.get(1);

        for (MdlContext ctx : MdlManager.getContextMap().values()) {
            MdlContextStamped ctxStamped = (MdlContextStamped) ctx;
            for (Map.Entry<MdlContextStamped.TransactionInfo, Map<MdlKey, MdlTicket>> tickets : ctxStamped.getTickets()
                .entrySet()) {
                MdlContextStamped.TransactionInfo info = tickets.getKey();
                for (Map.Entry<MdlKey, MdlTicket> requests : tickets.getValue().entrySet()) {
                    MdlKey key = requests.getKey();
                    MdlTicket ticket = requests.getValue();
                    if (null == key) {
                        continue;
                    }
                    if (schemaName != null && !schemaName.equalsIgnoreCase(key.getDbName())) {
                        continue;
                    }
                    if (tableName != null && !tableName.equalsIgnoreCase(key.getTableName())) {
                        continue;
                    }

                    ByteString formatSql = info.getSql();
                    String sql = null;
                    if (formatSql != null && formatSql.length() > 1024) {
                        sql = formatSql.substring(0, 1024 - 3) + "...";
                    } else if (formatSql != null) {
                        sql = formatSql.toString();
                    }

                    Object[] row;
                    if (null == ticket) {
                        row = new Object[] {
                            ctxStamped.getConnId(),
                            info.getTrxId(), info.getTraceId(), key.getDbName(), key.getTableName(),
                            null, null, null, info.getFrontend(), sql};
                    } else {
                        row = new Object[] {
                            ctxStamped.getConnId(),
                            info.getTrxId(), info.getTraceId(), key.getDbName(), key.getTableName(),
                            ticket.getType().name(), ticket.getDuration().name(),
                            ticket.isValidate() ? 1 : 0, info.getFrontend(), sql};
                    }

                    resultCursor.addRow(row);
                }
            }
        }

        return resultCursor;
    }

    private ArrayResultCursor buildResultCursor() {
        ArrayResultCursor resultCursor = new ArrayResultCursor("METADATA_LOCKS");

        resultCursor.addColumn("CONN_ID", DataTypes.StringType);
        resultCursor.addColumn("TRX_ID", DataTypes.IntegerType);
        resultCursor.addColumn("TRACE_ID", DataTypes.StringType);
        resultCursor.addColumn("SCHEMA", DataTypes.StringType);
        resultCursor.addColumn("TABLE", DataTypes.StringType);
        resultCursor.addColumn("TYPE", DataTypes.StringType);
        resultCursor.addColumn("DURATION", DataTypes.StringType);
        resultCursor.addColumn("VALIDATE", DataTypes.IntegerType);
        resultCursor.addColumn("FRONTEND", DataTypes.StringType);
        resultCursor.addColumn("SQL", DataTypes.StringType);

        resultCursor.initMeta();

        return resultCursor;
    }

}

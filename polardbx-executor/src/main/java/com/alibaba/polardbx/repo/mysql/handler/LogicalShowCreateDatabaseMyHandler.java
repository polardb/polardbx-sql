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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCreateDatabase;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;

/**
 * @author moyi
 */
public class LogicalShowCreateDatabaseMyHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowCreateDatabaseMyHandler.class);

    public LogicalShowCreateDatabaseMyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowCreateDatabase showCreateDatabase = (SqlShowCreateDatabase) show.getNativeSqlNode();
        final String databaseName = RelUtils.lastStringValue(showCreateDatabase.getDbName());
        final DbInfoRecord dbInfo = DbInfoManager.getInstance().getDbInfo(databaseName);
        if (dbInfo == null) {
            throw new TddlNestableRuntimeException("Unknown database " + databaseName);
        }
        final long databaseId = dbInfo.id;
        final LocalityManager lm = LocalityManager.getInstance();

        ArrayResultCursor result = new ArrayResultCursor("Create Database");
        result.addColumn("Database", DataTypes.StringType);
        result.addColumn("Create Database", DataTypes.StringType);
        result.initMeta();

        StringBuilder builder = new StringBuilder();
        builder.append("CREATE DATABASE `").append(databaseName).append('`');
        LocalityInfo locality = lm.getLocalityOfDb(databaseId);

        int dbType = -1;
        String partitionMode = null;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            metaDbConn.setAutoCommit(true);
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(databaseName);
            dbType = dbInfoRecord.dbType;
            partitionMode = DbInfoManager.getPartitionModeByDbType(dbType);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }

        StringBuilder optiionBuilder = new StringBuilder();
        if (!StringUtils.isEmpty(partitionMode)) {
            optiionBuilder.append("MODE = \'").append(partitionMode).append("\'");
        }

        if (locality != null) {
            optiionBuilder.append(" LOCALITY = \"").append(locality.getLocality()).append("\"");
        }

        String showCreateDbStr = "";
        String optionContentStr = optiionBuilder.toString();
        if (!StringUtils.isEmpty(optionContentStr)) {
            showCreateDbStr = String.format("%s /* %s */", builder.toString(), optionContentStr);
        }
        // TODO: charset, collation
        result.addRow(new Object[] {databaseName, showCreateDbStr});

        return result;
    }

}

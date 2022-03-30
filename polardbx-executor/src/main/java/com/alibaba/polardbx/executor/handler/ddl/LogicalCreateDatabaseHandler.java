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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.DdlVisibility;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.MySQLCharsetDDLValidator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.topology.CreateDbInfo;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.DbNameUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateDatabase;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateDatabase;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;

/**
 * @author chenmo.cm
 */
public class LogicalCreateDatabaseHandler extends HandlerCommon {

    public LogicalCreateDatabaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalCreateDatabase createDatabase = (LogicalCreateDatabase) logicalPlan;
        final SqlCreateDatabase sqlCreateDatabase = (SqlCreateDatabase) createDatabase.getNativeSqlNode();
        final LocalityManager lm = LocalityManager.getInstance();

        String dbName = sqlCreateDatabase.getDbName().getSimple();
        if (!DbNameUtil.validateDbName(dbName, KeyWordsUtil.isKeyWord(dbName))) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Failed to create database because the dbName[%s] is invalid", dbName));
        }

        int normalDbCnt = DbTopologyManager.getNormalDbCountFromMetaDb();
        int maxDbCnt = DbTopologyManager.maxLogicalDbCount;
        if (normalDbCnt >= maxDbCnt) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format(
                    "Failed to create database because there are too many databases, the max count of database is %s",
                    maxDbCnt));
        }

        String charset = Optional.ofNullable(sqlCreateDatabase.getCharSet())
            .orElse(DbTopologyManager.defaultCharacterSetForCreatingDb);
        String collate = Optional.ofNullable(sqlCreateDatabase.getCollate())
            .orElse(CharsetName.getDefaultCollationName(charset));

        if (!MySQLCharsetDDLValidator.checkCharsetSupported(charset, collate)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "Failed to create database because the charset[%s] or collate[%s] is not supported",
                    charset, collate));
        }

        String locality = Strings.nullToEmpty(sqlCreateDatabase.getLocality());


        if (!MySQLCharsetDDLValidator.checkCharset(charset)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format(
                    "Unknown character set: %s",
                    charset));
        }

        if (!StringUtils.isEmpty(collate)) {

            if (!MySQLCharsetDDLValidator.checkCollation(collate)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format(
                        "Unknown collation: %s",
                        collate));
            }

            if (!MySQLCharsetDDLValidator.checkCharsetCollation(charset, collate)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format(
                        "Unknown character set and collation: %s %s",
                        charset, collate));
            }

        }

        String partitionMode = Strings.nullToEmpty(sqlCreateDatabase.getPartitionMode());
        boolean isCreateIfNotExists = sqlCreateDatabase.isIfNotExists();
        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        long socketTimeoutVal = socketTimeout == null ? -1 : socketTimeout;

        String shardDbCountEachStorageInstStr =
            (String) executionContext.getExtraCmds()
                .get(ConnectionProperties.SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT);
        int shardDbCountEachStorageInst = -1;
        if (shardDbCountEachStorageInstStr != null) {
            shardDbCountEachStorageInst = Integer.valueOf(shardDbCountEachStorageInstStr);
        }

        // choose dn by locality
        LocalityDesc localityDesc = LocalityDesc.parse(locality);
        Predicate<StorageInfoRecord> predLocality = (x -> localityDesc.matchStorageInstance(x.getInstanceId()));
        int dbType = decideDbType(partitionMode, executionContext);
        CreateDbInfo createDbInfo = DbTopologyManager.initCreateDbInfo(
            dbName, charset, collate, locality, predLocality, dbType,
            isCreateIfNotExists, socketTimeoutVal, shardDbCountEachStorageInst);
        long dbId = DbTopologyManager.createLogicalDb(createDbInfo);

        CdcManagerHelper.getInstance()
            .notifyDdl(dbName, null, sqlCreateDatabase.getKind().name(), executionContext.getOriginSql(),
                DdlVisibility.Public, executionContext.getExtraCmds());

        if (!localityDesc.isEmpty()) {
            lm.setLocalityOfDb(dbId, locality);
        }
        return new AffectRowCursor(new int[] {1});
    }

    protected int decideDbType(String partitionMode, ExecutionContext executionContext) {

        String defaultPartMode = DbTopologyManager.getDefaultPartitionMode();
        int dbType = -1;
        if (StringUtils.isEmpty(partitionMode)) {
            partitionMode = defaultPartMode;
        }
        if (partitionMode.equalsIgnoreCase(DbInfoManager.MODE_AUTO) || partitionMode.equalsIgnoreCase(
            DbInfoManager.MODE_PARTITIONING)) {
            dbType = DbInfoRecord.DB_TYPE_NEW_PART_DB;
        } else if (partitionMode.equalsIgnoreCase(DbInfoManager.MODE_DRDS) || partitionMode.equalsIgnoreCase(
            DbInfoManager.MODE_SHARDING)) {
            dbType = DbInfoRecord.DB_TYPE_PART_DB;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to create database with invalid mode=" + partitionMode);
        }
        return dbType;
    }

}

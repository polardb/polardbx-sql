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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;

import javax.sql.DataSource;
import java.sql.Connection;

/**
 * @author chenghui.lch
 */
public class SystemDbHelper {
    private static final Logger logger = LoggerFactory.getLogger(SystemDbHelper.class);

    // Properties for Default Db
    public static final String DEFAULT_DB_NAME = "polardbx";
    public static final String DEFAULT_META_DB_NAME = "METADB";
    public static final String DEFAULT_DB_CHARSET = "utf8";
    public static final String DEFAULT_DB_APP_NAME = "polardbx_app";
    public static final String DEFAULT_DB_PHY_NAME = "polardbx";
    public static final String DEFAULT_DB_GROUP_NAME = "POLARDBX_SINGLE_GROUP";

    // Properties for InfoSchema Db
    public static final String INFO_SCHEMA_DB_NAME = "information_schema";
    public static final String INFO_SCHEMA_DB_CHARSET = "utf8";
    public static final String INFO_SCHEMA_DB_APP_NAME = "polardbx_info_schema_app";
    public static final String INFO_SCHEMA_DB_PHY_NAME = "polardbx_info_schema";
    public static final String INFO_SCHEMA_DB_GROUP_NAME = "INFORMATION_SCHEMA_SINGLE_GROUP";

    // Properties for Cdc Db
    public static final String CDC_DB_NAME = "__cdc__";
    public static final String CDC_DB_CHARSET = "utf8";

    private final static String[] buildInDb = {INFO_SCHEMA_DB_NAME, CDC_DB_NAME, DEFAULT_DB_NAME, DEFAULT_META_DB_NAME};
    private final static String[] buildInDbExcludeCdc = {INFO_SCHEMA_DB_NAME, DEFAULT_DB_NAME};

    public static void checkOrCreateDefaultDb(MetaDbDataSource metaDbDs) {
        String dbName = DEFAULT_DB_NAME;
        String phyDbName = DEFAULT_DB_PHY_NAME;
        String groupName = DEFAULT_DB_GROUP_NAME;
        String charset = DEFAULT_DB_CHARSET;
        DbTopologyManager.createInternalSystemDbIfNeed(metaDbDs, dbName, phyDbName, groupName, charset,
            DbInfoRecord.DB_TYPE_DEFAULT_DB);
    }

    public static void checkOrCreateInfoSchemaDb(MetaDbDataSource metaDbDs) {
        String dbName = INFO_SCHEMA_DB_NAME;
        String phyDbName = INFO_SCHEMA_DB_PHY_NAME;
        String groupName = INFO_SCHEMA_DB_GROUP_NAME;
        String charset = INFO_SCHEMA_DB_CHARSET;
        DbTopologyManager.createInternalSystemDbIfNeed(metaDbDs, dbName, phyDbName, groupName, charset,
            DbInfoRecord.DB_TYPE_SYSTEM_DB);
    }

    /**
     * added by ziyang.lb
     * 几点说明如下：
     * 1.cdc db的主要作用是支持心跳和ddl打标，逻辑Binlog系统会强依赖该数据库才能正常运转
     * 2.cdc db虽然也是一个系统库，但dbtype不能复用DB_TYPE_SYSTEM_DB，因为其物理分片并不是分布在metadb
     * 3.cdc db必须满足一个要求：在每个Storage实例上，要至少分布一个物理库分片，这样才能保证逻辑Binlog系统对心跳和打标等对齐
     */
    public static void checkOrCreateCdcDb(MetaDbDataSource metaDbDs) {
        DataSource dataSource = metaDbDs.getDataSource();
        try (Connection metaDbConn = dataSource.getConnection()) {
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            DbInfoRecord dbInfo = dbInfoAccessor.getDbInfoByDbName(SystemDbHelper.CDC_DB_NAME);
            if (!(dbInfo != null && dbInfo.dbStatus == DbInfoRecord.DB_STATUS_RUNNING)) {
                CreateDbInfo createDbInfo = DbTopologyManager
                    .initCreateDbInfo(SystemDbHelper.CDC_DB_NAME, SystemDbHelper.CDC_DB_CHARSET, "", "",
                        null, DbInfoRecord.DB_TYPE_CDC_DB, true, -1, 1);
                createDbInfo.dbType = DbInfoRecord.DB_TYPE_CDC_DB;
                DbTopologyManager.createLogicalDb(createDbInfo);
            }
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, "failed to create cdc db, err is " +
                ex.getMessage());
        }
    }

    /**
     * Identify if one schema being build-in
     */
    public static boolean isDBBuildIn(String schema) {
        for (String n : buildInDb) {
            if (n.equalsIgnoreCase(schema)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isDBBuildInExceptCdc(String schema) {
        for (String n : buildInDbExcludeCdc) {
            if (n.equalsIgnoreCase(schema)) {
                return true;
            }
        }
        return false;
    }
}

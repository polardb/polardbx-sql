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

package com.alibaba.polardbx.gms.listener.impl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.text.MessageFormat;
import java.text.ParseException;

/**
 * @author chenghui.lch
 */
public class MetaDbDataIdBuilder {

    /**
     * {0} : instId
     */
    public static final MessageFormat INST_CONFIG_DATA_ID = new MessageFormat("polardbx.inst.config.{0}");

    /**
     * {0} : instId
     */
    public static final MessageFormat SERVER_INFO_DATA_ID = new MessageFormat("polardbx.server.info.{0}");

    /**
     * {0} : instId
     */
    public static final MessageFormat STORAGE_INFO_DATA_ID = new MessageFormat("polardbx.storage.info.{0}");

    /**
     * {0} : instId
     */
    public static final MessageFormat QUARANTINE_CONFIG_DATA_ID = new MessageFormat("polardbx.quarantine.config.{0}");

    /**
     * {0} : instId
     */
    public static final MessageFormat INST_LOCK_DATA_ID = new MessageFormat("polardbx.inst.lock.{0}");

    /**
     * {0} : instId
     */
    public static final MessageFormat CCL_RULE_DATA_ID = new MessageFormat("polardbx.ccl.rule.{0}");

    /**
     * {0} : instId, {1}: dbName, {2}: groupName
     */
    public static final MessageFormat GROUP_CONFIG_DATA_ID = new MessageFormat("polardbx.group.config.{0}.{1}.{2}");

    /**
     * {0} : instId, {1}: dbName, {2}: groupName
     */
    public static final MessageFormat GROUP_STORAGE_CONFIG_DATA_ID =
        new MessageFormat("polardbx.group.storage.config.{0}.{1}.{2}");

    public static final String INST_INFO_DATA_ID = "polardbx.inst.info";

    public static final String PRIVILEGE_INFO_DATA_ID = "polardbx.privilege.info";

    public static final String LOGIN_ERROR_DATA_ID = "polardbx.login.error.limit.config";

    public static final String LBAC_SECURITY_DATA_ID = "polardbx.security.lbac";

    public static final String DB_INFO_DATA_ID = "polardbx.db.info";

    public static final String LOCALITY_INFO_DATA_ID = "polardbx.locality.info";

    public static final String ENCDB_RULE_DATA_ID = "polardbx.encdb.rule";
    public static final String ENCDB_KEY_DATA_ID = "polardbx.encdb.key";
    public static final String STORAGE_POOL_INFO_DATA_ID = "polardbx.storage.pool.info";

    public static final MessageFormat DB_COMPLEX_TASK_DATA_ID = new MessageFormat("polardbx.db.complextask.{0}");
    /**
     * {0} : instId
     */
    public static final MessageFormat VARIABLE_CONFIG_DATA_ID = new MessageFormat("polardbx.variable.config.{0}");

    /**
     * {0} : dbName
     */
    public static final MessageFormat DB_TOPOLOGY_DATA_ID = new MessageFormat("polardbx.db.topology.{0}");

    /**
     * use for meta db lock
     */
    public static final String METADB_LOCK_DATA_ID = "polardbx.metadb.lock";

    /**
     * each dbname has a data_id
     */
    public static final String TABLE_LIST_DATA_ID_PREFIX = "polardbx.meta.tables.";

    /**
     * each dbName & tbName has a data_id
     */
    public static final String TABLE_DATA_ID_PREFIX = "polardbx.meta.table.";

    /**
     * {0} : dbName
     */
    private static final MessageFormat TABLE_LIST_DATA_ID = new MessageFormat(TABLE_LIST_DATA_ID_PREFIX + "{0}");

    /**
     * {0} : dbName, {1}: tbName
     */
    private static final MessageFormat TABLE_DATA_ID = new MessageFormat(TABLE_DATA_ID_PREFIX + "{0}.{1}");

    /**
     * {0} : dbName
     */
    public static final MessageFormat COLUMNAR_TABLE_LIST_DATA_ID = new MessageFormat("polardbx.meta.columnars.{0}");

    /**
     * {0} : dbName, {1}: primaryTbName
     */
    private static final MessageFormat COLUMNAR_TABLE_DATA_ID = new MessageFormat("polardbx.meta.columnar.{0}.{1}");

    /**
     * each dbname has a data_id
     */
    public static final String DDL_JOB_LIST_DATA_ID_PREFIX = "polardbx.meta.ddl.jobs.";

    /**
     * each dbName & ddlJob has a data_id
     */
    public static final String DDL_JOB_DATA_ID_PREFIX = "polardbx.meta.ddl.job.";

    /**
     * {0} : dbName
     */
    private static final MessageFormat DDL_JOB_LIST_DATA_ID = new MessageFormat(DDL_JOB_LIST_DATA_ID_PREFIX + "{0}");

    /**
     * {0} : dbName, {1}: ddlJobId
     */
    private static final MessageFormat DDL_JOB_DATA_ID = new MessageFormat(DDL_JOB_DATA_ID_PREFIX + "{0}.{1}");

    /**
     * File storage engine dataId for the whole instance.
     */
    public static final String FILE_STORAGE_INFO_DATA_ID = "polardbx.file.storage.info";

    public static final String CDC_SYSTEM_CONFIG_DATA_ID = "polardbx.cdc.config";

    public static String formatDataId(String dataId) {
        return dataId.toLowerCase();
    }

    public static String getTableListDataId(String schemaName) {
        return formatDataId(TABLE_LIST_DATA_ID.format(new Object[] {schemaName}));
    }

    public static String getTableDataIdPrefix(String schemaName) {
        return formatDataId(TABLE_DATA_ID.format(new Object[] {schemaName, ""}));
    }

    public static String getTableDataId(String schemaName, String tableName) {
        return formatDataId(TABLE_DATA_ID.format(new Object[] {schemaName, tableName}));
    }

    public static String getColumnarTableListDataId(String schemaName) {
        return COLUMNAR_TABLE_LIST_DATA_ID.format(new Object[] {schemaName});
    }

    public static String getColumnarTableDataIdPrefix(String schemaName) {
        return COLUMNAR_TABLE_DATA_ID.format(new Object[] {schemaName, ""});
    }

    public static String getColumnarDataId(String schemaName, String tableName) {
        return COLUMNAR_TABLE_DATA_ID.format(new Object[] {schemaName, tableName});
    }

    public static String getServerInfoDataId(String instId) {
        return formatDataId(SERVER_INFO_DATA_ID.format(new Object[] {instId}));
    }

    public static String getStorageInfoDataId(String instId) {
        return formatDataId(STORAGE_INFO_DATA_ID.format(new Object[] {instId}));
    }

    public static String getStoragePoolInfoDataId() {
        return STORAGE_POOL_INFO_DATA_ID;
    }

    public static String getInstConfigDataId(String instId) {
        return formatDataId(INST_CONFIG_DATA_ID.format(new Object[] {instId}));
    }

    public static String getQuarantineConfigDataId(String instId) {
        return formatDataId(QUARANTINE_CONFIG_DATA_ID.format(new Object[] {instId}));
    }

    public static String getInstLockDataId(String instId) {
        return formatDataId(INST_LOCK_DATA_ID.format(new Object[] {instId}));
    }

    public static String getDbTopologyDataId(String dbName) {
        return formatDataId(DB_TOPOLOGY_DATA_ID.format(new Object[] {dbName.toLowerCase()}));
    }

    public static String resolveDbTopologyDataId(String dataId) {
        try {
            Object[] objs = DB_TOPOLOGY_DATA_ID.parse(dataId);
            if (objs == null || objs.length != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "Invalid dataId: " + dataId);
            }
            return (String) objs[0];
        } catch (ParseException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "Invalid dataId: " + dataId);
        }
    }

    public static String getGroupConfigDataId(String instId, String dbName, String groupName) {

        String grpConfigDataId = GROUP_CONFIG_DATA_ID.format(new Object[] {instId, dbName, groupName});
        return formatDataId(grpConfigDataId);
    }

    public static String getConfigStorageDataId(String instId, String dbName, String groupName) {

        String grpConfigDataId = GROUP_STORAGE_CONFIG_DATA_ID.format(new Object[] {instId, dbName, groupName});
        return formatDataId(grpConfigDataId);
    }

    public static String getLocalityInfoDataId() {
        return formatDataId(LOCALITY_INFO_DATA_ID);
    }

    public static String getPrivilegeInfoDataId() {
        return formatDataId(PRIVILEGE_INFO_DATA_ID);
    }

    public static String getLBACSecurityDataId() {
        return formatDataId(LBAC_SECURITY_DATA_ID);
    }

    public static String getMetadbLockDataId() {
        return formatDataId(METADB_LOCK_DATA_ID);
    }

    public static String getDbInfoDataId() {
        return formatDataId(DB_INFO_DATA_ID);
    }

    public static String getInstInfoDataId() {
        return formatDataId(INST_INFO_DATA_ID);
    }

    public static String getCclRuleDataId(String instId) {
        return formatDataId(CCL_RULE_DATA_ID.format(new Object[] {instId}));
    }

    public static String getDbComplexTaskDataId(String dbName) {
        return formatDataId(DB_COMPLEX_TASK_DATA_ID.format(new Object[] {dbName.toLowerCase()}));
    }

    public static String getVariableConfigDataId(String instId) {
        return formatDataId(VARIABLE_CONFIG_DATA_ID.format(new Object[] {instId}));
    }

    public static String getFileStorageInfoDataId() {
        return formatDataId(FILE_STORAGE_INFO_DATA_ID);
    }

    public static String getCdcSystemConfigDataId() {
        return formatDataId(CDC_SYSTEM_CONFIG_DATA_ID);
    }
}

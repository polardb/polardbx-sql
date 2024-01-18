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

package com.alibaba.polardbx.gms.locality;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author moyi
 */
public class StoragePoolInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(DbGroupInfoAccessor.class);
    private static final String STORAGE_POOL_INFO_TABLE = GmsSystemTables.STORAGE_POOL_INFO;

    protected static final String ALL_COLUMNS =
        "`id`, `name`, `gmt_created`, `gmt_modified`, `dn_ids`, `undeletable_dn_id`, `extras`";
    protected static final String SELECT_ALL_STORAGE_POOL =
        "SELECT " + ALL_COLUMNS + " FROM `" + STORAGE_POOL_INFO_TABLE + "` ORDER BY id";

    protected static final String INSERT_STORAGE_POOL =
        "INSERT INTO " + STORAGE_POOL_INFO_TABLE + "(`name`, `dn_ids`, `undeletable_dn_id`)" + "VALUES(?,?,?)";

    protected static final String DELETE_STORAGE_INFO =
        "DELETE FROM " + STORAGE_POOL_INFO_TABLE + " where `name` = ?";

    protected static final String TRUNCATE_STORAGEPOOL_INFO =
        "DELETE FROM " + STORAGE_POOL_INFO_TABLE + " where 1 = ?";
    protected static final String UPDATE_STORAGE_POOL =
        "UPDATE " + STORAGE_POOL_INFO_TABLE + " set `dn_ids` = ?, `undeletable_dn_id` = ? " + " WHERE `name` = ?";

    protected static final String UPDATE_STORAGE_POOL_INFO_NAME =
        "UPDATE " + STORAGE_POOL_INFO_TABLE + " set `name` = ? " + " WHERE `name` = ?";

    public List<StoragePoolInfoRecord> getAllStoragePoolInfoRecord() {
        try {
            List<StoragePoolInfoRecord> records = MetaDbUtil.query(
                SELECT_ALL_STORAGE_POOL, StoragePoolInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query system table " + STORAGE_POOL_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                STORAGE_POOL_INFO_TABLE,
                e.getMessage());
        }
    }

    public void addNewStoragePoolInfo(String name, String dnIds, String undeletableDnId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, dnIds);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, undeletableDnId);
            MetaDbUtil.insert(INSERT_STORAGE_POOL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to query system table " + STORAGE_POOL_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                STORAGE_POOL_INFO_TABLE,
                e.getMessage());
        }
    }

    public void deleteStoragePoolInfo(String name) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, name);
            MetaDbUtil.insert(DELETE_STORAGE_INFO, params, connection);
        } catch (Exception e) {
            logger.error("Failed to query system table " + STORAGE_POOL_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                STORAGE_POOL_INFO_TABLE,
                e.getMessage());
        }
    }

    public void truncateStoragePoolInfo() {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, 1);
            MetaDbUtil.update(TRUNCATE_STORAGEPOOL_INFO, params, connection);
        } catch (Exception e) {
            logger.error("Failed to query system table " + STORAGE_POOL_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                STORAGE_POOL_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updateStoragePoolInfoName(String originalName, String targetName) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, targetName);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, originalName);
            MetaDbUtil.insert(UPDATE_STORAGE_POOL_INFO_NAME, params, connection);
        } catch (Exception e) {
            logger.error("Failed to query system table " + STORAGE_POOL_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                STORAGE_POOL_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updateStoragePoolInfo(String name, String dnIds, String undeletableDnId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, dnIds);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, undeletableDnId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, name);
            MetaDbUtil.update(UPDATE_STORAGE_POOL, params, connection);
        } catch (Exception e) {
            logger.error("Failed to query system table " + STORAGE_POOL_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                STORAGE_POOL_INFO_TABLE,
                e.getMessage());
        }
    }

}
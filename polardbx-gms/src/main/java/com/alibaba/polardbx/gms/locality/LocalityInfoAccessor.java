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
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author moyi
 */
public class LocalityInfoAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(DbGroupInfoAccessor.class);
    private static final String LOCALITY_INFO_TABLE = GmsSystemTables.LOCALITY_INFO;

    protected static final String SELECT_ALL_LOCALITY =
        "SELECT * FROM `" + LOCALITY_INFO_TABLE + "` ORDER BY OBJECT_TYPE ASC";
    protected static final String SELECT_DEFAULT_LOCALITY =
        "SELECT * FROM `" + LOCALITY_INFO_TABLE + "` WHERE OBJECT_TYPE = 0";
    protected static final String SELECT_LOCALITY_OF_DATABASE =
        "SELECT * FROM `" + LOCALITY_INFO_TABLE + "` WHERE OBJECT_TYPE = 1 and object_id=?";
    protected static final String INSERT_LOCALITY_INFO =
        "REPLACE INTO `" + LOCALITY_INFO_TABLE + "` (object_type, object_id, primary_zone, locality) " +
            "VALUES(?,?,?,?)";
    protected static final String DELETE_LOCALITY =
        "DELETE FROM `" + LOCALITY_INFO_TABLE + "` WHERE OBJECT_TYPE = ? AND OBJECT_ID = ?";
    protected static final String UPDATE_LOCALITY =
        "REPLACE INTO `" + LOCALITY_INFO_TABLE + "` (object_type, object_id, locality) VALUES(?,?,?)";
    protected static final String UPDATE_PRIMARY_ZONE =
        "REPLACE INTO `" + LOCALITY_INFO_TABLE + "` (object_type, object_id, primary_zone) VALUES(?,?,?)";

    public LocalityInfoRecord getDefaultLocalityInfo() {
        return getLocalityInfo(LocalityInfoRecord.LOCALITY_TYPE_DEFAULT, LocalityInfoRecord.LOCALITY_ID_DEFAULT);
    }

    public LocalityInfoRecord getLocalityInfo(int objectType, long objectId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, objectType);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, objectId);
            List<LocalityInfoRecord> records =
                MetaDbUtil.query(SELECT_LOCALITY_OF_DATABASE, LocalityInfoRecord.class, connection);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        } catch (Exception e) {
            logger.error("Failed to query system table " + LOCALITY_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                LOCALITY_INFO_TABLE,
                e.getMessage());
        }
    }

    public List<LocalityInfoRecord> getAllLocality() {
        try {
            List<LocalityInfoRecord> records = MetaDbUtil.query(
                SELECT_ALL_LOCALITY, LocalityInfoRecord.class, connection);
            return records;
        } catch (Exception e) {
            logger.error("Failed to query system table " + LOCALITY_INFO_TABLE, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "query",
                LOCALITY_INFO_TABLE,
                e.getMessage());
        }
    }

    public void insertLocalityInfo(int objectType, long objectId, String primaryZone, String locality) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, objectType);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, objectId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, primaryZone);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, locality);
            DdlMetaLogUtil.logSql(INSERT_LOCALITY_INFO, params);
            MetaDbUtil.update(INSERT_LOCALITY_INFO, params, connection);
            logger.debug(String.format("insert locality_info: type=%d,id=%d,primary_zone=%s,locality=%s",
                objectType, objectId, primaryZone, locality));
        } catch (Exception e) {
            logger.error("Failed to insert the system table '" + LOCALITY_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "update",
                LOCALITY_INFO_TABLE,
                e.getMessage());
        }
    }

    public void updatePrimaryZone(int objectType, long objectId, String primaryZone) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, objectType);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, objectId);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, primaryZone);
            MetaDbUtil.insert(UPDATE_PRIMARY_ZONE, params, connection);
            logger.info(String.format("update locality_info: type=%d,id=%d,primary_zone=%s",
                objectType, objectId, primaryZone));
        } catch (Exception e) {
            logger.error("Failed to update the system table '" + LOCALITY_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "update",
                LOCALITY_INFO_TABLE,
                e.getMessage());
        }
    }

    public int deleteLocality(int objectType, long objectId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setInt, objectType);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, objectId);
            return MetaDbUtil.delete(DELETE_LOCALITY, params, connection);
        } catch (Exception e) {
            logger.error("Failed to delete from the system table '" + LOCALITY_INFO_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                e,
                "delete",
                LOCALITY_INFO_TABLE,
                e.getMessage());
        }
    }
}
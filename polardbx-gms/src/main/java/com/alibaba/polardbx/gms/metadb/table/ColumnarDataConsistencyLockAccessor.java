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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.base.Preconditions;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_DATA_CONSISTENCY_LOCK;

public class ColumnarDataConsistencyLockAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private static final String COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE = wrap(COLUMNAR_DATA_CONSISTENCY_LOCK);

    private static final String INSERT_NEW_LOCK = "insert into " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE
        + "(entity_id, owner_id, state, last_owner) values(?, ?, ?, ?)";

    private static final String FORCE_INDEX_UK = " force index (uk_entity_id) ";

    private static final String WHERE_QUERY_LOCK = " where entity_id = ?";
    private static final String WHERE_QUERY_CHECK_LOCK = " where entity_id = ? and owner_id = ?";
    private static final String WHERE_UPDATE_ACQUIRE_LOCK = " where entity_id = ? and owner_id = ? and state = ?";
    private static final String WHERE_UPDATE_RELEASE_LOCK = " where entity_id = ? and owner_id = ? and state > 0";

    private static final String ALL_COLUMNS =
        "id, entity_id, owner_id, state, last_owner, last_owner_change, create_time, update_time";

    private static final String SELECT_ALL_LOCK =
        "select " + ALL_COLUMNS + " from " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE;
    private static final String SELECT_LOCK =
        "select " + ALL_COLUMNS + " from " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + WHERE_QUERY_LOCK;
    private static final String SELECT_LOCK_FOR_UPDATE =
        "select " + ALL_COLUMNS + " from " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + FORCE_INDEX_UK + WHERE_QUERY_LOCK
            + " for update";

    private static final String SELECT_LOCK_FOR_READ =
        "select " + ALL_COLUMNS + " from " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + FORCE_INDEX_UK
            + WHERE_QUERY_CHECK_LOCK
            + " lock in share mode";

    private static final String UPDATE_LOCK_OWNER =
        "update " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + FORCE_INDEX_UK + " set state = ? , owner_id = ?"
            + WHERE_UPDATE_ACQUIRE_LOCK;

    private static final String UPDATE_LOCK_STATE =
        "update " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + FORCE_INDEX_UK + " set state = state + ?"
            + WHERE_UPDATE_ACQUIRE_LOCK;

    private static final String UPDATE_RELEASE_LOCK =
        "update " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + FORCE_INDEX_UK + " set state = state - ?"
            + WHERE_UPDATE_RELEASE_LOCK;

    private static final String DELETE_CLEAR_LOCK = "delete from" + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE;

    public int insert(ColumnarDataConsistencyLockRecord record) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            int index = 0;
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, record.entityId);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, record.ownerId);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, record.state);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, record.lastOwner);

            return MetaDbUtil.insert(INSERT_NEW_LOCK, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a new record into " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + ". ", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert into",
                COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE,
                e.getMessage());
        }
    }

    public List<ColumnarDataConsistencyLockRecord> queryAll() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        return query(SELECT_ALL_LOCK, COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE, ColumnarDataConsistencyLockRecord.class,
            params);
    }

    public List<ColumnarDataConsistencyLockRecord> query(String entityId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, Preconditions.checkNotNull(entityId));
        return query(SELECT_LOCK, COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE, ColumnarDataConsistencyLockRecord.class,
            params);
    }

    public List<ColumnarDataConsistencyLockRecord> queryForUpdate(String entityId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, Preconditions.checkNotNull(entityId));
        return query(SELECT_LOCK_FOR_UPDATE, COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE,
            ColumnarDataConsistencyLockRecord.class, params);
    }

    public List<ColumnarDataConsistencyLockRecord> queryForRead(String entityId, int ownerId) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, Preconditions.checkNotNull(entityId));
        MetaDbUtil.setParameter(2, params, ParameterMethod.setInt, ownerId);
        return query(SELECT_LOCK_FOR_READ, COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE,
            ColumnarDataConsistencyLockRecord.class, params);
    }

    /**
     * Lock is free, acquire lock by updating its state and ownerId
     *
     * @param newState new state
     * @param newOwnerId new owner
     * @param entityId lock entity id
     * @param oldOwnerId old owner
     * @return 1 if succeed
     */
    public int updateLockOwner(long newState, int newOwnerId, String entityId, int oldOwnerId) {
        int index = 0;
        Map<Integer, ParameterContext> params = new HashMap<>(5);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, newState);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, newOwnerId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, entityId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, oldOwnerId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, 0L);
        return update(UPDATE_LOCK_OWNER, COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE, params);
    }

    /**
     * Lock is holding by current owner, acquire lock by increasing its state
     *
     * @param stateAdd new state = current state + stateAdd
     * @param entityId lock entity id
     * @param oldOwnerId current owner
     * @param oldState current state
     * @return 1 if succeed
     */
    public int updateLockState(long stateAdd, String entityId, int oldOwnerId, long oldState) {
        int index = 0;
        Map<Integer, ParameterContext> params = new HashMap<>(4);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, stateAdd);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, entityId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, oldOwnerId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, oldState);
        return update(UPDATE_LOCK_STATE, COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE, params);
    }

    /**
     * Lock is holding by current owner, release lock by decreasing its state
     *
     * @param stateSub new state = current state - stateSub
     * @param entityId lock entity id
     * @param oldOwnerId current owner
     * @return 1 if success
     */
    public int updateLockRelease(long stateSub, String entityId, int oldOwnerId) {
        int index = 0;
        Map<Integer, ParameterContext> params = new HashMap<>(3);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, stateSub);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, entityId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, oldOwnerId);
        return update(UPDATE_RELEASE_LOCK, COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE, params);
    }

    /**
     * Clear all lock record
     *
     * @return number of lock released
     */
    public int clearLock() {
        try {
            return MetaDbUtil.delete(DELETE_CLEAR_LOCK, connection);
        } catch (Exception e) {
            LOGGER.error(
                "Failed to delete system table " + COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE + " sql: " + DELETE_CLEAR_LOCK,
                e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                COLUMNAR_DATA_CONSISTENCY_LOCK_TABLE,
                e.getMessage());
        }
    }
}

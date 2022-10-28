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

package com.alibaba.polardbx.gms.metadb.lease;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class LeaseAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaseAccessor.class);

    public static final String LEASE = wrap(GmsSystemTables.LEASE);

    private static final String ALL_COLUMNS = "`id`, `schema_name`, `lease_holder`, `lease_key`, `start_at`, `last_modified`, `expire_at`, `ttl_millis`";

    private static final String ACQUIRE_LEASE_SQL =
        "insert ignore into " + LEASE +
            " (" + ALL_COLUMNS + " ) "
            + " values (null, ?, ?, ?, REPLACE(unix_timestamp(current_timestamp(3)),'.',''), start_at, start_at + ?, ?)";

    private static final String SELECT_FULL =
        "select " + ALL_COLUMNS + " ";
    private static final String FROM_TABLE = " from " + LEASE;
    private static final String SELECT_LEASE_BY_HOLDER_AND_KEY_SQL =
        SELECT_FULL + FROM_TABLE + " where lease_holder = ? and lease_key = ?";
    private static final String DELETE_LEASE_BY_HOLDER_AND_KEY_SQL =
        "delete " + FROM_TABLE + " where lease_holder = ? and lease_key = ?";

    private static final String SELECT_REMAINING_TIME =
        "select expire_at - REPLACE(unix_timestamp(current_timestamp(3)),'.','') from " + LEASE +
            " where lease_holder = ? and lease_key = ?";

    private static final String EXPIRATION_TEST = " expire_at <= REPLACE(unix_timestamp(current_timestamp(3)),'.','') ";
    private static final String DELETE_IF_LEASE_KEY_EXPIRED_SQL =
        "delete from " + LEASE + " where " + EXPIRATION_TEST + " and lease_key = ?";

    private static final String DELETE_ALL_IF_LEASE_KEY_EXPIRED_SQL =
        "delete from " + LEASE + " where " + EXPIRATION_TEST;

    private static final String EXTEND_SQL = "update " + LEASE +
        " set last_modified = REPLACE(unix_timestamp(current_timestamp(3)),'.',''), expire_at = last_modified + ttl_millis "
        + " where lease_holder = ? and lease_key = ? and expire_at > REPLACE(unix_timestamp(current_timestamp(3)),'.','')";

    private static final String WHERE_SCHEMA_NAME = " where `schema_name` = ?";
    private static final String DELETE_BY_SCHEMA_NAME = "delete from " + LEASE + WHERE_SCHEMA_NAME;


    public int acquire(LeaseRecord record) {
        try {
            DdlMetaLogUtil.logSql(ACQUIRE_LEASE_SQL, record.buildParamsForInsert());
            return MetaDbUtil.insert(ACQUIRE_LEASE_SQL, record.buildParamsForInsert(), connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to insert a new record into " + LEASE, "insert into", e);
        }
    }

    public Optional<LeaseRecord> queryByHolderAndKey(String leaseHolder, String leaseKey) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {leaseHolder, leaseKey});
            List<LeaseRecord> records =
                MetaDbUtil.query(SELECT_LEASE_BY_HOLDER_AND_KEY_SQL, params, LeaseRecord.class, connection);
            if (CollectionUtils.isNotEmpty(records)) {
                return Optional.of(records.get(0));
            }
            return Optional.empty();
        } catch (Exception e) {
            throw logAndThrow("Failed to query from " + LEASE, "query from", e);
        }
    }

    public long selectRemainingTime(String leaseHolder, String leaseKey) {
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            long remaining = 0;
            stmt = connection.prepareStatement(SELECT_REMAINING_TIME);
            stmt.setString(1, leaseHolder);
            stmt.setString(2, leaseKey);
            rs = stmt.executeQuery();
            if (rs.next()) {
                remaining = rs.getLong(1);
            }
            return remaining;
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, ex, ex.getMessage());
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } catch (Throwable ex) {
                // Ignore
            }
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Throwable ex) {
                // Ignore
            }
        }
    }



    public int extendByHolderAndKey(String leaseHolder, String leaseKey) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {
                    leaseHolder,
                    leaseKey
                });
            DdlMetaLogUtil.logSql(EXTEND_SQL, params);
            return MetaDbUtil.update(EXTEND_SQL, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + LEASE + " for leaseKey: " + leaseKey,
                "delete from", e);
        }
    }

    public int deleteIfExpired(String leaseKey){
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {leaseKey});
            DdlMetaLogUtil.logSql(DELETE_IF_LEASE_KEY_EXPIRED_SQL, params);
            return MetaDbUtil.delete(DELETE_IF_LEASE_KEY_EXPIRED_SQL, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + LEASE + " for leaseKey: " + leaseKey,
                "delete from", e);
        }
    }

    public int deleteAllIfExpired(){
        try {
            return MetaDbUtil.delete(DELETE_ALL_IF_LEASE_KEY_EXPIRED_SQL, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + LEASE, "delete from", e);
        }
    }

    public int deleteByHolderAndKey(String leaseHolder, String leaseKey) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {leaseHolder, leaseKey});
            DdlMetaLogUtil.logSql(DELETE_LEASE_BY_HOLDER_AND_KEY_SQL, params);
            return MetaDbUtil.delete(DELETE_LEASE_BY_HOLDER_AND_KEY_SQL, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + LEASE, "query from", e);
        }
    }

    public int deleteAll(String schemaName) {
        try {
            final Map<Integer, ParameterContext> params =
                MetaDbUtil.buildParameters(ParameterMethod.setString, new String[] {schemaName});
            DdlMetaLogUtil.logSql(DELETE_BY_SCHEMA_NAME, params);
            return MetaDbUtil.delete(DELETE_BY_SCHEMA_NAME, params, connection);
        } catch (Exception e) {
            throw logAndThrow("Failed to delete from " + LEASE + " for schemaName: " + schemaName,
                "delete from",
                e);
        }
    }

    private TddlRuntimeException logAndThrow(String errMsg, String action, Exception e) {
        LOGGER.error(errMsg, e);
        return new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, action,
            LEASE, e.getMessage());
    }

}

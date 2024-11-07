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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.COLUMNAR_LEASE;

public class ColumnarLeaseAccessor extends AbstractAccessor {
    private static final String COLUMNAR_LEASE_TABLE = wrap(COLUMNAR_LEASE);

    private static final String INIT = "insert into " + COLUMNAR_LEASE_TABLE
        + " (`id`, `owner`, `lease`) values (1, ?, ?)";

    // 抢占租期，如果当前时间大于上个租期结束时间+500ms（时钟漂移）或者owner没有变更，即可抢占
    private static final String ELECT = "update " + COLUMNAR_LEASE_TABLE
        + " set `owner` = ?, `lease` = ?"
        + " where `id` = 1 and (`lease` + 500 < ? or `owner` = ?)";

    // 强制抢占租期
    private static final String FORCE_ELECT = "update " + COLUMNAR_LEASE_TABLE
        + " set `owner` = ?, `lease` = ?"
        + " where `id` = 1";

    // 取消租期
    private static final String FORCE_CANCEL = "delete from " + COLUMNAR_LEASE_TABLE + "where `id` = 1 and `owner` = ?";

    // 续租，当前的leader续租自己的租期，只允许lease增长
    private static final String RENEW = "update " + COLUMNAR_LEASE_TABLE
        + " set `lease` = ?"
        + " where `id` = 1 and `owner` = ? and `lease` < ?";

    // update node info
    private static final String UPDATE_NODE = "update " + COLUMNAR_LEASE_TABLE
        + " set `lease` = ?"
        + " where `id` != 1 and `owner` = ?";

    // insert
    private static final String INSERT_NODE = "insert into " + COLUMNAR_LEASE_TABLE
        + " (`lease`, `owner`) values (?, ?)";

    // remove outdated node info
    private static final String CLEANUP = "delete from " + COLUMNAR_LEASE_TABLE
        + " where `id` != 1 and `lease` < ?";

    // get now nodes
    private static final String GET_NODES = "select `id`, `owner`, `lease` from " + COLUMNAR_LEASE_TABLE
        + " where `id` != 1 and `lease` >= ?";

    // 加锁查询上次的leader的lease时间
    private static final String LOCK_LEADER_LEASE = "select `id`, `owner`, `lease` from " + COLUMNAR_LEASE_TABLE
        + " where `id` = 1 for update";

    public boolean elect(final String owner, final long nowUTC, final long leaseMs) {
        try {
            // try insert first
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, nowUTC + leaseMs);

            try {
                final int inserts = MetaDbUtil.insert(INIT, params, connection);
                if (1 == inserts) {
                    return true;
                }
            } catch (SQLIntegrityConstraintViolationException e) {
                // ignore
            }

            // try update
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, nowUTC);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, owner);

            final int updates = MetaDbUtil.update(ELECT, params, connection);
            return 1 == updates;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * 返回上一个leader的租约时间, 拆成三步，insert； select for update ； update
     */
    public long forceElectInsert(final String owner, final long nowUTC, final long leaseMs) {
        try {
            // try insert first
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, nowUTC + leaseMs);

            try {
                final int inserts = MetaDbUtil.insert(INIT, params, connection);
                if (1 == inserts) {
                    return 0;
                }
            } catch (SQLIntegrityConstraintViolationException e) {
                // ignore
                return -1;
            }
            return -1;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnarLeaseRecord> forceElectSelectForUpdate() {
        try {
            return MetaDbUtil.query(LOCK_LEADER_LEASE, null, ColumnarLeaseRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void forceElectUpdate(final String owner, final long nowUTC, final long leaseMs) {
        try {
            // try insert first
            final Map<Integer, ParameterContext> params = new HashMap<>(4);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setLong, nowUTC + leaseMs);

            final int updates = MetaDbUtil.update(FORCE_ELECT, params, connection);
            if (updates != 1) {
                throw new RuntimeException("Failed to force elect");
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public boolean renew(final String owner, final long nowUTC, final long leaseMs) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(3);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC + leaseMs);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, owner);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, nowUTC + leaseMs);

            final int updates = MetaDbUtil.update(RENEW, params, connection);
            return 1 == updates;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public List<ColumnarLeaseRecord> refresh(final String owner, final long nowUTC, final long leaseMs) {
        try {
            // update myself first
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC + leaseMs);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, owner);
            final int updates = MetaDbUtil.update(UPDATE_NODE, params, connection);
            if (0 == updates) {
                // need insert myself
                MetaDbUtil.update(INSERT_NODE, params, connection);
                // just ignore result
            }

            // cleanup outdated
            params.clear();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC);
            MetaDbUtil.update(CLEANUP, params, connection);

            // get now nodes
            return MetaDbUtil.query(GET_NODES, params, ColumnarLeaseRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int delete(String owner) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(2);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, owner);
            return MetaDbUtil.delete(FORCE_CANCEL, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}

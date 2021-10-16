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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class InstLockAccessor extends AbstractAccessor {

    private static final String INST_LOCK_TABLE = GmsSystemTables.INST_LOCK;

    private static final String SELECT_INST_LOCK_BY_INST_ID =
        "select * from `" + INST_LOCK_TABLE + "` where inst_id=?";

    private static final String DELETE_INST_LOCK_BY_INST_ID =
        "delete from `" + INST_LOCK_TABLE + "` where inst_id=?";

    public InstLockRecord getInstLockByInstId(String instId) {
        try {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
            List<InstLockRecord> rs = MetaDbUtil
                .query(InstLockAccessor.SELECT_INST_LOCK_BY_INST_ID, params, InstLockRecord.class, connection);
            if (rs.size() == 0) {
                return null;
            }
            return rs.get(0);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                INST_LOCK_TABLE,
                e.getMessage());
        }
    }

    public void deleteInstLockByInstId(String instId) throws SQLException {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, instId);
        MetaDbUtil.delete(DELETE_INST_LOCK_BY_INST_ID, params, this.connection);
    }

}

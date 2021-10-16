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

package com.alibaba.polardbx.cdc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoAccessor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;

/**
 * created by ziyang.lb
 **/
public class CdcDbLock {
    public static void acquireCdcDbLockByForUpdate(Connection metaDbConn) throws SQLException {
        if (metaDbConn.getAutoCommit()) {
            metaDbConn.setAutoCommit(false);
        }
        DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
        dbInfoAccessor.setConnection(metaDbConn);
        dbInfoAccessor.getDbInfoByDbNameForUpdate(CDC_DB_NAME);
    }

    public static void releaseCdcDbLockByCommit(Connection metaDbConn) throws SQLException {
        metaDbConn.commit();
    }

    public static void processInLock(Supplier<?> supplier) {
        try (Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {
            // acquire Cdc Lock by for update, to avoiding concurrent update cdc meta info
            metaDbLockConn.setAutoCommit(false);
            try {
                acquireCdcDbLockByForUpdate(metaDbLockConn);
            } catch (Throwable ex) {
                throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GMS_GENERIC, ex,
                    "Get metaDb lock timeout during update cdc group info, please retry");
            }

            supplier.get();

            releaseCdcDbLockByCommit(metaDbLockConn);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }
}

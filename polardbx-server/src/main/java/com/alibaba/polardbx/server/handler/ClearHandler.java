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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParseClear;
import com.alibaba.polardbx.server.response.ClearFileSystemCache;
import com.alibaba.polardbx.server.response.ClearPartitionsHeatmapCache;
import com.alibaba.polardbx.server.response.ClearPlanCache;
import com.alibaba.polardbx.server.response.ClearProcedureCache;
import com.alibaba.polardbx.server.response.ClearSQLSlow;
import com.alibaba.polardbx.server.response.ClearStoredFunctionCache;
import com.alibaba.polardbx.server.util.LogUtils;

/**
 * @author xianmao.hexm
 */
public final class ClearHandler {

    public static boolean handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            switch (ServerParseClear.parse(stmt, offset)) {
            case ServerParseClear.SLOW:
                return ClearSQLSlow.response(c, hasMore);
            case ServerParseClear.PLANCACHE:
                return ClearPlanCache.response(c, hasMore);
            case ServerParseClear.OSSCACHE:
                return ClearFileSystemCache.response(c, Engine.OSS, false, hasMore);
            case ServerParseClear.ALLCACHE:
                return ClearFileSystemCache.response(c, null, true, hasMore);
            case ServerParseClear.NFS_CACHE:
                return ClearFileSystemCache.response(c, Engine.NFS, false, hasMore);
            case ServerParseClear.EXTERNAL_DISK_CACHE:
                return ClearFileSystemCache.response(c, Engine.EXTERNAL_DISK, false, hasMore);
            case ServerParseClear.HEATMAP_CACHE:
                return ClearPartitionsHeatmapCache.response(c, hasMore);
            case ServerParseClear.PROCEDURE_CACHE:
                return ClearProcedureCache.response(c, hasMore);
            case ServerParseClear.FUNCTION_CACHE:
                return ClearStoredFunctionCache.response(c, hasMore);
            default:
                recordSql = false;
                return c.execute(stmt, hasMore);
            }
        } catch (Throwable ex) {
            sqlEx = ex;
            throw ex;
        } finally {
            if (recordSql) {
                LogUtils.recordSql(c, stmt, sqlEx);
            }
        }
    }

}

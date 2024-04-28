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

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParseResize;
import com.alibaba.polardbx.server.response.ResizeProcedureCache;
import com.alibaba.polardbx.server.response.ResizeStoredFunctionCache;
import com.alibaba.polardbx.server.util.LogUtils;

public final class ResizeHandler {
    public static boolean handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            int rs = ServerParseResize.parse(stmt, offset);
            switch (rs & 0xff) {
            case ServerParseResize.PROCEDURE_CACHE:
                return ResizeProcedureCache.response(stmt, c, rs >>> 8, hasMore);
            case ServerParseResize.FUNCTION_CACHE:
                return ResizeStoredFunctionCache.response(stmt, c, rs >>> 8, hasMore);
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

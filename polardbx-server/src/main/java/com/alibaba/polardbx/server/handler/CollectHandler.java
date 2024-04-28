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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParseCollect;
import com.alibaba.polardbx.server.response.CollectStatistic;
import com.alibaba.polardbx.server.util.LogUtils;

public class CollectHandler {
    private static final Logger logger = LoggerFactory.getLogger("STATISTICS");

    public static boolean handle(ByteString stmt, ServerConnection c, int offset) {
        int rs = ServerParseCollect.parse(stmt, offset);
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            switch (rs & 0xff) {
            case ServerParseCollect.STATISTIC:
                return CollectStatistic.response(c);
            default:
                recordSql = false;
                return c.execute(stmt, false);
            }
        } catch (Throwable ex) {
            logger.error(ex);
            sqlEx = ex;
            throw ex;
        } finally {
            if (recordSql) {
                LogUtils.recordSql(c, stmt, sqlEx);
            }
        }
    }
}

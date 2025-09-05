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
import com.alibaba.polardbx.server.parser.ServerParseShow;
import com.alibaba.polardbx.server.response.ShowArchive;
import com.alibaba.polardbx.server.response.ShowCacheFileStats;
import com.alibaba.polardbx.server.response.ShowCacheStats;
import com.alibaba.polardbx.server.response.ShowColumnarOffset;
import com.alibaba.polardbx.server.response.ShowColumnarStatus;
import com.alibaba.polardbx.server.response.ShowColumnarVersion;
import com.alibaba.polardbx.server.response.ShowCompatibilityLevel;
import com.alibaba.polardbx.server.response.ShowConnection;
import com.alibaba.polardbx.server.response.ShowDatabases;
import com.alibaba.polardbx.server.response.ShowErrors;
import com.alibaba.polardbx.server.response.ShowFileStorage;
import com.alibaba.polardbx.server.response.ShowFullConnection;
import com.alibaba.polardbx.server.response.ShowFullDatabases;
import com.alibaba.polardbx.server.response.ShowFileStorage;
import com.alibaba.polardbx.server.response.ShowFullConnection;
import com.alibaba.polardbx.server.response.ShowFullDatabases;
import com.alibaba.polardbx.server.response.ShowFileStorage;
import com.alibaba.polardbx.server.response.ShowGitCommit;
import com.alibaba.polardbx.server.response.ShowHelp;
import com.alibaba.polardbx.server.response.ShowMdlDeadlockDetectionStatus;
import com.alibaba.polardbx.server.response.ShowMemoryPool;
import com.alibaba.polardbx.server.response.ShowMpp;
import com.alibaba.polardbx.server.response.ShowNode;
import com.alibaba.polardbx.server.response.ShowParametric;
import com.alibaba.polardbx.server.response.ShowSqlEngineAlert;
import com.alibaba.polardbx.server.response.ShowStatistic;
import com.alibaba.polardbx.server.response.ShowWarnings;
import com.alibaba.polardbx.server.response.ShowWorkload;
import com.alibaba.polardbx.server.util.LogUtils;

/**
 * @author xianmao.hexm
 */
public final class ShowHandler {

    /**
     * @return true:no error packet
     */
    public static boolean handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        int rs = ServerParseShow.parse(stmt, offset);
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            switch (rs & 0xff) {
            case ServerParseShow.DATABASES:
                return ShowDatabases.response(c, hasMore);
            case ServerParseShow.NODE:
                return ShowNode.execute(c);
            case ServerParseShow.CONNECTION:
                return ShowConnection.execute(c, hasMore);
            case ServerParseShow.WARNINGS:
                return ShowWarnings.execute(c, hasMore);
            case ServerParseShow.ERRORS:
                return ShowErrors.execute(c, hasMore);
            case ServerParseShow.HELP:
                return ShowHelp.execute(c);
            case ServerParseShow.GIT_COMMIT:
                return ShowGitCommit.execute(c);
            case ServerParseShow.STATISTIC:
                return ShowStatistic.execute(c);
            case ServerParseShow.MEMORYPOOL:
                return ShowMemoryPool.execute(c);
            case ServerParseShow.MDL_DEADLOCK_DETECTION:
                return ShowMdlDeadlockDetectionStatus.response(c);
            case ServerParseShow.MPP:
                return ShowMpp.execute(c);
            case ServerParseShow.WORKLOAD:
                return ShowWorkload.execute(c);
            case ServerParseShow.PARAMETRIC:
                return ShowParametric.response(c);
            case ServerParseShow.CACHE_STATS:
                return ShowCacheStats.execute(c);
            case ServerParseShow.CACHE_FILE_STATS:
                return ShowCacheFileStats.execute(c);
            case ServerParseShow.ARCHIVE:
                return ShowArchive.execute(c);
            case ServerParseShow.FILE_STORAGE:
                return ShowFileStorage.execute(c);
            case ServerParseShow.FULL_DATABASES:
                return ShowFullDatabases.response(c, hasMore);
            case ServerParseShow.FULL_CONNECTION:
                return ShowFullConnection.execute(c, hasMore);
            case ServerParseShow.COLUMNAR_VERSION:
                return ShowColumnarVersion.execute(c);
            case ServerParseShow.COLUMNAR_STATUS:
                return ShowColumnarStatus.execute(c, stmt, offset, false);
            case ServerParseShow.FULL_COLUMNAR_STATUS:
                return ShowColumnarStatus.execute(c, stmt, offset, true);
                case ServerParseShow.SQL_ENGINE_ALERT:
                return ShowSqlEngineAlert.execute(c);
            case ServerParseShow.COLUMNAR_OFFSET:
                return ShowColumnarOffset.execute(c);
            case ServerParseShow.COMPATIBILITY_LEVEL:
                return ShowCompatibilityLevel.execute(c, stmt);
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

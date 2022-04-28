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

import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.parser.ServerParseShow;
import com.alibaba.polardbx.server.response.ShowArchive;
import com.alibaba.polardbx.server.response.ShowCacheStats;
import com.alibaba.polardbx.server.response.ShowConnection;
import com.alibaba.polardbx.server.response.ShowDatabases;
import com.alibaba.polardbx.server.response.ShowErrors;
import com.alibaba.polardbx.server.response.ShowFileStorage;
import com.alibaba.polardbx.server.response.ShowGitCommit;
import com.alibaba.polardbx.server.response.ShowHelp;
import com.alibaba.polardbx.server.response.ShowMpp;
import com.alibaba.polardbx.server.response.ShowMdlDeadlockDetectionStatus;
import com.alibaba.polardbx.server.response.ShowMemoryPool;
import com.alibaba.polardbx.server.response.ShowNode;
import com.alibaba.polardbx.server.response.ShowParametric;
import com.alibaba.polardbx.server.response.ShowStatistic;
import com.alibaba.polardbx.server.response.ShowStorage;
import com.alibaba.polardbx.server.response.ShowWarnings;
import com.alibaba.polardbx.server.response.ShowWorkload;
import com.alibaba.polardbx.server.util.LogUtils;
import com.alibaba.polardbx.druid.sql.parser.ByteString;

/**
 * @author xianmao.hexm
 */
public final class ShowHandler {

    public static void handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore) {
        int rs = ServerParseShow.parse(stmt, offset);
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            switch (rs & 0xff) {
            case ServerParseShow.DATABASES:
                ShowDatabases.response(c, hasMore);
                break;
            case ServerParseShow.NODE:
                ShowNode.execute(c);
                break;
            case ServerParseShow.CONNECTION:
                ShowConnection.execute(c, hasMore);
                break;
            case ServerParseShow.WARNINGS:
                ShowWarnings.execute(c, hasMore);
                break;
            case ServerParseShow.ERRORS:
                ShowErrors.execute(c, hasMore);
                break;
            case ServerParseShow.HELP:
                ShowHelp.execute(c);
                break;
            case ServerParseShow.GIT_COMMIT:
                ShowGitCommit.execute(c);
                break;
            case ServerParseShow.STATISTIC:
                ShowStatistic.execute(c);
                break;
            case ServerParseShow.MEMORYPOOL:
                ShowMemoryPool.execute(c);
                break;
            case ServerParseShow.STORAGE_REPLICAS:
                ShowStorage.execute(c, true);
                break;
            case ServerParseShow.STORAGE:
                ShowStorage.execute(c);
                break;
            case ServerParseShow.MDL_DEADLOCK_DETECTION:
                ShowMdlDeadlockDetectionStatus.response(c);
            case ServerParseShow.MPP:
                ShowMpp.execute(c);
                break;
            case ServerParseShow.WORKLOAD:
                ShowWorkload.execute(c);
                break;
            case ServerParseShow.PARAMETRIC:
                ShowParametric.response(c);
                break;
            case ServerParseShow.CACHE_STATS:
                ShowCacheStats.execute(c);
                break;
            case ServerParseShow.ARCHIVE:
                ShowArchive.execute(c);
                break;
            case ServerParseShow.FILE_STORAGE:
                ShowFileStorage.execute(c);
                break;
            default:
                c.execute(stmt, hasMore);
                recordSql = false;
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

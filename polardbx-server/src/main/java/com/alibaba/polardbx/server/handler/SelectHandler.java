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
import com.alibaba.polardbx.server.parser.ServerParseSelect;
import com.alibaba.polardbx.server.response.SelectCurrentTransId;
import com.alibaba.polardbx.server.response.SelectCurrentTransPolicy;
import com.alibaba.polardbx.server.response.SelectDatabase;
import com.alibaba.polardbx.server.response.SelectExtractTraceId;
import com.alibaba.polardbx.server.response.SelectLastTxcId;
import com.alibaba.polardbx.server.response.SelectLiteralNumber;
import com.alibaba.polardbx.server.response.SelectSequenceBenchmark;
import com.alibaba.polardbx.server.response.SelectSessionTxReadOnly;
import com.alibaba.polardbx.server.response.SelectTsoTimestamp;
import com.alibaba.polardbx.server.response.SelectUser;
import com.alibaba.polardbx.server.response.SelectVersion;
import com.alibaba.polardbx.server.response.SelectVersionComment;
import com.alibaba.polardbx.server.util.LogUtils;

/**
 * @author xianmao.hexm
 */
public final class SelectHandler {

    public static void handle(ByteString stmt, ServerConnection c, int offs, boolean hasMore) {
        Object[] exData = new Object[2];
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            switch (ServerParseSelect.parse(stmt, offs, exData)) {
            case ServerParseSelect.VERSION_COMMENT:
                SelectVersionComment.response(c, hasMore);
                break;
            case ServerParseSelect.DATABASE:
                SelectDatabase.response(c, hasMore);
                break;
            case ServerParseSelect.USER:
                SelectUser.response(c, hasMore);
                break;
            case ServerParseSelect.VERSION:
                SelectVersion.response(c, hasMore);
                break;
            case ServerParseSelect.LASTTXCXID:
                SelectLastTxcId.response(exData, c, hasMore);
                break;
            case ServerParseSelect.SESSION_TX_READ_ONLY:
                SelectSessionTxReadOnly.response(c, hasMore);
                break;
            case ServerParseSelect.CURRENT_TRANS_ID:
                SelectCurrentTransId.response(c, hasMore);
                break;
            case ServerParseSelect.CURRENT_TRANS_POLICY:
                SelectCurrentTransPolicy.response(c, hasMore);
                break;
            case ServerParseSelect.LITERAL_NUMBER:
                SelectLiteralNumber.response(c, hasMore, (Long) exData[0]);
                break;
            case ServerParseSelect.TSO_TIMESTAMP:
                SelectTsoTimestamp.response(c, hasMore);
                break;
            case ServerParseSelect.EXTRACT_TRACE_ID:
                SelectExtractTraceId.response(c, hasMore, (Long) exData[0]);
                break;
            case ServerParseSelect.SEQ_NEXTVAL_BENCHMARK:
                SelectSequenceBenchmark.response(c, hasMore, exData, true);
                recordSql = false;
                break;
            case ServerParseSelect.SEQ_SKIP_BENCHMARK:
                SelectSequenceBenchmark.response(c, hasMore, exData, false);
                recordSql = false;
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

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
import com.alibaba.polardbx.server.response.SelectColumnarFile;
import com.alibaba.polardbx.server.response.SelectEncdbProcessMessage;
import com.alibaba.polardbx.server.response.SelectCompatibilityLevel;
import com.alibaba.polardbx.server.response.SelectPolardbVersion;
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

    public static boolean handle(ByteString stmt, ServerConnection c, int offs, boolean hasMore) {
        Object[] exData = new Object[2];
        boolean recordSql = true;
        Throwable sqlEx = null;
        try {
            switch (ServerParseSelect.parse(stmt, offs, exData)) {
            case ServerParseSelect.VERSION_COMMENT:
                return SelectVersionComment.response(c, hasMore);
            case ServerParseSelect.DATABASE:
                return SelectDatabase.response(c, hasMore);
            case ServerParseSelect.USER:
                return SelectUser.response(c, hasMore);
            case ServerParseSelect.VERSION:
                return SelectVersion.response(c, hasMore);
            case ServerParseSelect.POLARDB_VERSION:
                return SelectPolardbVersion.response(c, hasMore);
            case ServerParseSelect.LASTTXCXID:
                return SelectLastTxcId.response(exData, c, hasMore);
            case ServerParseSelect.SESSION_TX_READ_ONLY:
                return SelectSessionTxReadOnly.response(c, hasMore);
            case ServerParseSelect.CURRENT_TRANS_ID:
                return SelectCurrentTransId.response(c, hasMore);
            case ServerParseSelect.CURRENT_TRANS_POLICY:
                return SelectCurrentTransPolicy.response(c, hasMore);
            case ServerParseSelect.LITERAL_NUMBER:
                return SelectLiteralNumber.response(c, hasMore, (Long) exData[0]);
            case ServerParseSelect.TSO_TIMESTAMP:
                return SelectTsoTimestamp.response(c, hasMore);
            case ServerParseSelect.EXTRACT_TRACE_ID:
                return SelectExtractTraceId.response(c, hasMore, (Long) exData[0]);
            case ServerParseSelect.SEQ_NEXTVAL_BENCHMARK:
                recordSql = false;
                return SelectSequenceBenchmark.response(c, hasMore, exData, true);
            case ServerParseSelect.SEQ_SKIP_BENCHMARK:
                recordSql = false;
                return SelectSequenceBenchmark.response(c, hasMore, exData, false);
            case ServerParseSelect.COMPATIBILITY_LEVEL:
                return SelectCompatibilityLevel.execute(c, stmt);
            case ServerParseSelect.ENCDB_PROCESS_MESSAGE:
                return SelectEncdbProcessMessage.response(c, hasMore, (String) exData[0]);
            case ServerParseSelect.COLUMNAR_FILE:
                return SelectColumnarFile.execute(c, hasMore, stmt);
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

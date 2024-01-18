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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.server.parser.ServerParse;
import com.alibaba.polardbx.server.parser.ServerParseClear;
import com.alibaba.polardbx.server.parser.ServerParseCollect;
import com.alibaba.polardbx.server.parser.ServerParseResize;
import com.alibaba.polardbx.server.parser.ServerParseSelect;
import com.alibaba.polardbx.server.parser.ServerParseShow;

public class PrepareUtils {
    public static boolean checkQueryType(ByteString sql, int rs) {
        // Only statements using ServerConnection.execute can be supported
        int commandCode = rs & 0xff;
        if (ServerParse.PREPARE_UNSUPPORTED_QUERY_TYPE.contains(commandCode)) {
            return false;
        }
        switch (commandCode) {
        case ServerParse.SHOW:
            return checkShow(sql, rs >>> 8);
        case ServerParse.CLEAR:
            return checkClear(sql, rs >>> 8);
        case ServerParse.SELECT:
            return checkSelect(sql, rs >>> 8);
        case ServerParse.COLLECT:
            return checkCollect(sql, rs >>> 8);
        case ServerParse.RESIZE:
            return checkResize(sql, rs >>> 8);
        default:
            return true;
        }
    }

    private static boolean checkShow(ByteString stmt, int offset) {
        int rs = ServerParseShow.parse(stmt, offset);
        int commandCode = rs & 0xff;
        return !ServerParseShow.PREPARE_UNSUPPORTED_SHOW_TYPE.contains(commandCode);
    }

    private static boolean checkClear(ByteString stmt, int offset) {
        int commandCode = ServerParseClear.parse(stmt, offset);
        return !ServerParseClear.PREPARE_UNSUPPORTED_CLEAR_TYPE.contains(commandCode);
    }

    private static boolean checkSelect(ByteString stmt, int offset) {
        Object[] exData = new Object[2];
        int commandCode = ServerParseSelect.parse(stmt, offset, exData);
        return !ServerParseSelect.PREPARE_UNSUPPORTED_SELECT_TYPE.contains(commandCode);
    }

    private static boolean checkCollect(ByteString stmt, int offset) {
        int rs = ServerParseCollect.parse(stmt, offset);
        int commandCode = rs & 0xff;
        return !ServerParseCollect.PREPARE_UNSUPPORTED_COLLECT_TYPE.contains(commandCode);
    }

    private static boolean checkResize(ByteString stmt, int offset) {
        int rs = ServerParseResize.parse(stmt, offset);
        int commandCode = rs & 0xff;
        return !ServerParseResize.PREPARE_UNSUPPORTED_RESIZE_TYPE.contains(commandCode);
    }
}

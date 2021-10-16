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

package com.alibaba.polardbx.server.ugly.hint;

import com.alibaba.polardbx.druid.sql.parser.ByteString;

/**
 * 增加DRDS全链路压测代码 Created by hongxi.chx on 17/3/6.
 */
public class EagleeyeTestHintParser {

    private static final String EXPLAIN = "explain ";

    private static final int MAX_LENGTH = 100;

    // 例如：/* 8be042ea14926712721273049d7805/0.1/1/ */
    public static boolean parseHint(ByteString sql) {
        int pos = 0;
        if (sql.length() > EXPLAIN.length() && EXPLAIN.equalsIgnoreCase(sql.substring(0, EXPLAIN.length()))) {
            pos += EXPLAIN.length();
        }
        if (sql.startsWith("/* ", pos)) {
            pos += 3;
        } else {
            return false;
        }

        // Match 1st part
        while (pos < MAX_LENGTH && pos < sql.length() && allow(sql.charAt(pos))) {
            pos++;
        }

        if (sql.charAt(pos) == '/') {
            pos++;
        } else {
            return false;
        }

        // Match 2nd part
        while (pos < MAX_LENGTH && pos < sql.length() && allow(sql.charAt(pos))) {
            pos++;
        }

        if (sql.charAt(pos) == '/') {
            pos++;
        } else {
            return false;
        }

        // Match 3rd part
        char flag;
        if (pos < MAX_LENGTH && pos < sql.length() && allow(flag = sql.charAt(pos))) {
            pos++;
        } else {
            return false;
        }

        if (sql.charAt(pos) == '/') {
            pos++;
        } else {
            return false;
        }

        if (sql.startsWith(" */", pos)) {
            pos += 3;
        } else {
            return false;
        }
        return flag == '1';
    }

    private static boolean allow(char c) {
        if (c >= '0' && c <= '9') {
            return true;
        } else if (c >= 'a' && c <= 'z') {
            return true;
        } else if (c >= 'A' && c <= 'Z') {
            return true;
        } else if (c == '.') {
            return true;
        }
        return false;
    }
}

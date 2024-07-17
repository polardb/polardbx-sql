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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLKeywords;

import java.util.Set;
import java.util.TreeSet;

/**
 * @author chenhui.lch
 */
public class KeyWordsUtil {
    private static Set<String> invalidKeyWords = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    static {
        invalidKeyWords.addAll(MySQLKeywords.DEFAULT_KEYWORDS.getKeywords().keySet());
        invalidKeyWords.add(SystemDbHelper.DEFAULT_DB_NAME);
        invalidKeyWords.add(SystemDbHelper.CDC_DB_NAME);
//        invalidKeyWords.add(SystemDbHelper.COLUMNAR_DB_NAME);
        for (Token token : Token.values()) {
            if (!invalidKeyWords.contains(token.name())) {
                invalidKeyWords.add(token.name());
            }
        }
    }

    public static boolean isKeyWord(String key) {
        if (key == null) {
            return false;
        }
        return invalidKeyWords.contains(key.toUpperCase());
    }
}

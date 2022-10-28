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

package com.alibaba.polardbx.optimizer.parse.mysql.lexer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author QIU Shuo
 */
public class MySQLKeywords {

    public static final MySQLKeywords     DEFAULT_KEYWORDS = new MySQLKeywords();

    private final Map<String, MySQLToken> keywords         = new HashMap<String, MySQLToken>(); // 全部是大写

    private MySQLKeywords(){
        for (MySQLToken type : MySQLToken.class.getEnumConstants()) {
            String name = type.name();
            if (name.startsWith("KW_")) {
                String kw = name.substring("KW_".length());
                keywords.put(kw, type);
            }
        }
        keywords.put("NULL", MySQLToken.LITERAL_NULL);
        keywords.put("FALSE", MySQLToken.LITERAL_BOOL_FALSE);
        keywords.put("TRUE", MySQLToken.LITERAL_BOOL_TRUE);
    }

    /**
     * @param keyUpperCase must be uppercase
     * @return <code>KeyWord</code> or {@link MySQLToken#LITERAL_NULL NULL} or
     * {@link MySQLToken#LITERAL_BOOL_FALSE FALSE} or
     * {@link MySQLToken#LITERAL_BOOL_TRUE TRUE}
     */
    public MySQLToken getKeyword(String keyUpperCase) {
        return keywords.get(keyUpperCase);
    }

    public boolean isKeyword(String keyUpperCase) {
        return keywords.containsKey(keyUpperCase);
    }

    public Map<String, MySQLToken> getKeywords() {
        return keywords;
    }
}

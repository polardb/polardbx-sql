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

import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.polardbx.druid.sql.parser.Token;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author busu
 * date: 2020/10/29 4:01 下午
 */
public class SqlKeywordMatchUtils {

    public static class MatchResult {

        public MatchResult(boolean matched, boolean matchParam) {
            this.matched = matched;
            this.matchParam = matchParam;
        }

        public boolean matched;
        public boolean matchParam;
    }

    public static final MatchResult DEFAULT_MATCH_RESULT = new MatchResult(true, false);

    /**
     * match keywords in the sql for normal statement and prepare statement.
     * considering the parameters which is for prepare statement.
     */
    public static MatchResult matchKeywords(String sql, Parameters params, List<String> keywords, int totalKeywordLen) {
        if (keywords == null || keywords.isEmpty()) {
            return DEFAULT_MATCH_RESULT;
        }
        assert params != null;

        int matchIndex = 0;
        int paramIndex = 1;
        int availableKeywordLen = totalKeywordLen;
        boolean notPruningByKeywordLen = false;
        if (!params.getCurrentParameter().isEmpty()) {
            notPruningByKeywordLen = true;
        }
        MyMySqlLexer lexer = new MyMySqlLexer(sql);
        boolean matchParam = false;
        do {
            lexer.nextToken();
            if (lexer.token() == Token.QUES) {
                //compare parameter value
                List<Map<Integer, ParameterContext>> batchParameters = params.getBatchParameters();

                for (Map<Integer, ParameterContext> map : batchParameters) {
                    ParameterContext parameterContext = map.get(paramIndex);
                    ParameterMethod parameterMethod = parameterContext.getParameterMethod();
                    switch (parameterMethod) {
                    case setArray:
                    case setAsciiStream:
                    case setBinaryStream:
                    case setBytes:
                    case setByte:
                    case setCharacterStream:
                    case setRef:
                    case setUnicodeStream:
                    case setTableName:
                        continue;
                    }
                    Object value = parameterContext.getValue();
                    if (value == null) {
                        value = "null";
                    }
                    if (StringUtils.equals(keywords.get(matchIndex), value.toString())) {
                        ++matchIndex;
                        if (!matchParam) {
                            matchParam = true;
                        }
                    }
                }
                ++paramIndex;

            } else {

                String word = stringVal(lexer);
                boolean compareResult =
                    isParamString(word) ?
                        StringUtils.equals(keywords.get(matchIndex), word.substring(1, word.length() - 1)) :
                        StringUtils.equalsIgnoreCase(keywords.get(matchIndex), word);
                if (compareResult) {
                    if (!notPruningByKeywordLen) {
                        availableKeywordLen -= word.length();
                    }
                    ++matchIndex;
                }

            }

        } while (matchIndex != keywords.size() && !lexer.isEOF() && (notPruningByKeywordLen
            || availableKeywordLen <= lexer.text.length() - lexer
            .pos()));

        return new MatchResult(matchIndex == keywords.size(), matchParam);
    }

    private static boolean isParamString(String stringVal) {
        if (StringUtils.isEmpty(stringVal)) {
            return false;
        }
        char firstCh = stringVal.charAt(0);
        return firstCh == '\'' || firstCh == '\"';
    }

    public static List<String> fetchWords(String sql) {
        if (StringUtils.isBlank(sql)) {
            return Collections.EMPTY_LIST;
        }
        List<String> words = Lists.newArrayList();
        MyMySqlLexer lexer = new MyMySqlLexer(sql);
        do {
            lexer.nextToken();
            String word = stringVal(lexer);
            if (StringUtils.isNotBlank(word)) {
                words.add(word);
            }
        } while (!lexer.isEOF());
        return words;
    }

    private static String stringVal(MyMySqlLexer lexer) {
        return lexer.subString(lexer.startPos(), lexer.pos() - lexer.startPos()).trim();
    }

    /**
     * extends MySqlLexer to access his attribute of startPos
     */
    private static class MyMySqlLexer extends MySqlLexer {

        public MyMySqlLexer(String sql) {
            super(sql);
        }

        public int startPos() {
            return super.startPos;
        }

    }

}

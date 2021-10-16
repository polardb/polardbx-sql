/*
 * Copyright 1999-2012 Alibaba Group.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * (created at 2011-6-17)
 */
package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.exception.SqlParserException;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken;
import com.alibaba.polardbx.optimizer.parse.util.CharTypes;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;

/**
 * @author fangwu 2017年4月19日
 * @since 5.2.26
 */
public final class HintParser {

    public static final String TDDL_HINT_PREFIX = "/*+TDDL(";
    public static final String TDDL_HINT_END = ")*/";

    public static final String TDDL_NEW_HINT_PREFIX = "/*+TDDL:";
    public static final String TDDL_NEW_HINT_END = "*/";

    private static final HintParser INSTANCE = new HintParser();

    public static final HintParser getInstance() {
        return INSTANCE;
    }

    public TddlHintSqlAnalysisResult parse(ByteString sql) throws SqlParserException {
        MySQLLexer lexer;
        try {
            lexer = new MySQLLexer(sql, false);

            String tddlHint = null;
            int tddlHintIndex = -1;
            int tddlHintEndIndex = -1;
            String tddlGroupHint = null;
            int tddlGroupHintIndex = -1;
            int tddlGroupHintEndIndex = -1;
            String tddlSimpleHint = null;
            int tddlSimpleHintIndex = -1;
            int tddlSimpleHintEndIndex = -1;
            String tddlMshaHint = null;
            int tddlMshaHintIndex = -1;
            int tddlMshaHintEndIndex = -1;

            while (true) {
                if (lexer.token() == MySQLToken.EOF) {
                    break;
                }

                switch (lexer.token()) {
                case TDDL_MSHA_HINT:
                    if (tddlMshaHint == null) {
                        tddlMshaHint = lexer.stringValue();
                        tddlMshaHintIndex = lexer.getHintStartIndex();
                        tddlMshaHintEndIndex = lexer.getCurrentIndex();
                    }
                    break;
                case TDDL_HINT:
                    if (tddlHint == null) {
                        tddlHint = lexer.stringValue();
                        tddlHintIndex = tddlSimpleHintIndex = lexer.getHintStartIndex();
                        tddlHintEndIndex = lexer.getCurrentIndex();
                    }
                    break;
                case TDDL_GROUP_HINT:
                    if (tddlGroupHint == null) {
                        tddlGroupHint = lexer.stringValue();
                        tddlGroupHintIndex = lexer.getHintStartIndex();
                        tddlGroupHintEndIndex = lexer.getCurrentIndex();
                    }
                    break;
                case TDDL_SIMPLE_HINT:
                    if (tddlSimpleHint == null) {
                        tddlSimpleHint = lexer.stringValue();
                        tddlSimpleHintIndex = lexer.getHintStartIndex();
                        tddlSimpleHintEndIndex = lexer.getCurrentIndex();
                    }
                    break;
                default:
                    break;
                }
                lexer.nextToken();
            }

            TddlHintSqlAnalysisResult result = new TddlHintSqlAnalysisResult(sql,
                tddlHint,
                tddlHintIndex,
                tddlHintEndIndex,
                tddlGroupHint,
                tddlGroupHintIndex,
                tddlGroupHintEndIndex,
                tddlSimpleHint,
                tddlSimpleHintIndex,
                tddlSimpleHintEndIndex,
                tddlMshaHint,
                tddlMshaHintIndex,
                tddlMshaHintEndIndex);

            return result;

        } catch (SQLSyntaxErrorException e) {
            throw new SqlParserException(e, e.getMessage());
        }
    }

    public int getAllHintCount(ByteString sql) {
        if (!containHint(sql)) {
            return 0;
        }
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getHintResultMap().size();
    }

    public String getTddlHint(ByteString sql) {
        if (containHint(sql)) {
            boolean useParser = useHintParser();
            if (!useParser) {
                return getBetween(sql, TDDL_HINT_PREFIX, TDDL_HINT_END, false);
            }
            TddlHintSqlAnalysisResult rs = this.parse(sql);
            return rs.getTddlHintStr();
        } else {
            return null;
        }
    }

    private int getTddlHintIndex(ByteString sql) {
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getTddlHintIndex();
    }

    private int getTddlHintEndIndex(ByteString sql) {
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getTddlHintEndIndex();
    }

    public String getTddlGroupHint(ByteString sql) {
        if (!containHint(sql)) {
            return null;
        }
        boolean useParser = useHintParser();
        if (!useParser) {
            return getBetween(sql, "/*+TDDL_GROUP({", "})*/", true);
        }
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        if (rs.getTddlGroupHintStr() != null) {
            return rs.getTddlGroupHintStr().trim();
        }
        return rs.getTddlGroupHintStr();
    }

    public String getTddlMshaHint(ByteString sql) {
        if (!containHint(sql)) {
            return null;
        }
        boolean useParser = useHintParser();
        if (!useParser) {
            return getBetween(sql, "/*+TDDL_MSHA(", ")*/", true);
        }
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        if (rs.getTddlMshaHintStr() != null) {
            return rs.getTddlMshaHintStr().trim();
        }
        return rs.getTddlMshaHintStr();
    }

    private boolean useHintParser() {
        return "TRUE".equalsIgnoreCase(System.getProperty(ConnectionProperties.HINT_PARSER_FLAG));
    }

    private int getTddlGroupHintIndex(ByteString sql) {
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getTddlGroupHintIndex();
    }

    private int getTddlGroupHintEndIndex(ByteString sql) {
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getTddlGroupHintEndIndex();
    }

    public String getTddlSimpleHint(ByteString sql) {
        if (!containHint(sql)) {
            return null;
        }
        boolean useParser = useHintParser();
        if (!useParser) {
            String hint = getBetween(sql, "/*TDDL:", "*/", false);
            if (hint == null) {
                hint = getBetween(sql, "/!TDDL:", "*/", false);
            }
            return hint;
        }

        TddlHintSqlAnalysisResult rs = this.parse(sql);
        if (rs.getTddlSimpleHintStr() != null) {
            return rs.getTddlSimpleHintStr();
        }
        return null;
    }

    private int getTddlSimpleHintIndex(ByteString sql) {
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getTddlSimpleHintIndex();
    }

    private int getTddlSimpleHintEndIndex(ByteString sql) {
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getTddlSimpleHintEndIndex();
    }

    public ByteString exchangeSimpleHint(String oldHint, String newHint, ByteString sql) {
        if (!containHint(sql)) {
            return sql;
        }
        boolean useParser = useHintParser();
        if (!useParser) {
            sql = replace(sql, "/!TDDL:", "/*TDDL:");
            if (!TStringUtil.isEmpty(newHint)) {
                sql = replaceOnce(sql, "/*TDDL:" + oldHint + "*/", newHint);
            } else {
                sql = removeBetweenWithSplitor(sql, "/*TDDL:", "*/");
            }
            return sql;
        }

        int oldHintIndex = HintParser.getInstance().getTddlSimpleHintIndex(sql);
        int oldHintEndIndex = HintParser.getInstance().getTddlSimpleHintEndIndex(sql);

        ByteBuffer buffer = ByteBuffer.allocate(sql.length() + (newHint != null ? newHint.length() : 1));
        sql.getBytes(0, oldHintIndex, buffer);
        if (!TStringUtil.isEmpty(newHint)) {
            buffer.put(newHint.getBytes());
        } else {
            buffer.put((byte) ' ');
        }
        sql.getBytes(oldHintEndIndex, sql.length(), buffer);
        return new ByteString(buffer.array(), 0, buffer.position(), sql.getCharset());
    }

    public ByteString removeTDDLHint(ByteString sql) {
        if (!containHint(sql)) {
            return sql;
        }
        boolean useParser = useHintParser();
        if (!useParser) {
            sql = removeBetweenWithSplitor(sql, TDDL_HINT_PREFIX, TDDL_HINT_END);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(sql.length());
            sql.getBytes(0, HintParser.getInstance().getTddlHintIndex(sql), buffer);
            buffer.put((byte) ' ');
            sql.getBytes(HintParser.getInstance().getTddlHintEndIndex(sql), sql.length(), buffer);
            sql = new ByteString(buffer.array(), 0, buffer.position(), sql.getCharset());
        }
        return sql;
    }

    public ByteString removeGroupHint(ByteString sql) {
        if (!containHint(sql)) {
            return sql;
        }
        boolean useParser = useHintParser();
        if (useParser) {
            ByteBuffer buffer = ByteBuffer.allocate(sql.length());
            sql.getBytes(0, HintParser.getInstance().getTddlGroupHintIndex(sql), buffer);
            buffer.put((byte) ' ');
            sql.getBytes(HintParser.getInstance().getTddlGroupHintEndIndex(sql), sql.length(), buffer);
            sql = new ByteString(buffer.array(), 0, buffer.position(), sql.getCharset());
        } else {
            sql = removeBetweenWithSplitor(sql, "/*+TDDL_GROUP({", "})*/");
        }
        return sql;
    }

    public ByteString getSqlRemovedHintStr(ByteString sql) {
        if (!containHint(sql)) {
            return null;
        }
        boolean useParser = useHintParser();
        if (!useParser) {
            return removeBetweenWithSplitor(sql, TDDL_HINT_PREFIX, TDDL_HINT_END);
        }
        TddlHintSqlAnalysisResult rs = this.parse(sql);
        return rs.getSqlRemovedHintStr();
    }

    public static boolean containHint(ByteString sql) {
        int i = 0;
        for (; i < sql.length(); ++i) {
            switch (sql.charAt(i)) {
            case ' ':
            case '\t':
            case '\r':
            case '\n':
                continue;
            }
            break;
        }

        return sql.startsWith("/*", i) || sql.startsWith("/!", i);
    }

    public static int comment(String sql, int offset) {
        int len = sql.length();
        int n = offset;
        switch (sql.charAt(n)) {
        case '/':
            if (len > ++n && sql.charAt(n++) == '*' && len > n + 1) {
                if (sql.charAt(n) != '!') {
                    for (int i = n; i < len; ++i) {
                        if (sql.charAt(i) == '*') {
                            int m = i + 1;
                            if (len > m && sql.charAt(m) == '/') {
                                return m;
                            }
                        }
                    }
                } else if (len > n + 6) {
                    // MySQL use 5 digits to indicate version. 50508
                    // means MySQL 5.5.8
                    if (CharTypes.isDigit(sql.charAt(n + 1))
                        && CharTypes.isDigit(sql.charAt(n + 2)) && CharTypes.isDigit(sql.charAt(n + 3)) && CharTypes
                        .isDigit(sql.charAt(n + 4)) && CharTypes.isDigit(sql.charAt(n + 5))) {
                        return n + 5;
                    }
                }

            }
            break;
        case '#':
            for (int i = n + 1; i < len; ++i) {
                if (sql.charAt(i) == '\n') {
                    return i;
                }
            }
            break;
        }
        return offset;
    }

    private static String getBetween(ByteString sql, String start, String end, boolean trim) {
        if (sql == null || start == null || end == null) {
            return null;
        }
        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return null;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return null;
        }
        String result = sql.substring(index0 + start.length(), index1);
        return trim ? result.trim() : result;
    }

    private static ByteString replaceOnce(ByteString text, String searchString, String replacement) {
        return replace(text, searchString, replacement, 1);
    }

    private static ByteString replace(ByteString text, String searchString, String replacement) {
        return replace(text, searchString, replacement, -1);
    }

    private static ByteString replace(ByteString text, String searchString, String replacement, int max) {
        if (!isEmpty(text) && !StringUtils.isEmpty(searchString) && replacement != null && max != 0) {
            int start = 0;
            int end = text.indexOf(searchString, start);
            if (end == -1) {
                return text;
            } else {
                int replLength = searchString.length();
                int increase = replacement.length() - replLength;
                increase = increase < 0 ? 0 : increase;
                increase *= max < 0 ? 16 : (max > 64 ? 64 : max);

                ByteBuffer buf = ByteBuffer.allocate(text.length() + increase);
                for (; end != -1; end = text.indexOf(searchString, start)) {
                    text.getBytes(start, end, buf);
                    buf.put(replacement.getBytes());
                    start = end + replLength;
                    --max;
                    if (max == 0) {
                        break;
                    }
                }

                buf.put(text.getBytes(start, text.length()));
                return new ByteString(buf.array(), 0, buf.position(), text.getCharset());
            }
        } else {
            return text;
        }
    }

    private static boolean isEmpty(ByteString str) {
        return str == null || str.length() == 0;
    }

    /**
     * 去除第一个start,end之间的字符串，包括start,end本身
     */
    public static ByteString removeBetweenWithSplitor(ByteString sql, String start, String end) {
        int index0 = sql.indexOf(start);
        if (index0 == -1) {
            return sql;
        }
        int index1 = sql.indexOf(end, index0);
        if (index1 == -1) {
            return sql;
        }
        ByteBuffer buffer = ByteBuffer.allocate(sql.length());
        sql.getBytes(0, index0, buffer);
        buffer.put((byte) ' ');
        sql.getBytes(index1 + end.length(), sql.length(), buffer);
        return new ByteString(buffer.array(), 0, buffer.position(), sql.getCharset());
    }
}

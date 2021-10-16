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

package com.alibaba.polardbx.optimizer.parse.mysql.ansiquote;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.sql.SQLSyntaxErrorException;
import java.util.List;

/**
 * ANSI_QUOTE 模式下将原始 SQL 转换为非 ANSI_QUOTE 模式下的 SQL
 *
 * 算法：
 *   将所有的由双引号标记的 identifier 转换为由反引号标记的 identifier，
 *   需要将起始双引号被替换为反引号，该 identifier 内部的转义双引号（由两个双引号组成）被替换为单双引号，
 *   反引号被替换转义反引号（由两个反引号组成）。
 *
 * @author arnkore 2017-08-30 15:08
 */
public class MySQLANSIQuoteTransformer extends MySQLLexer {
    // 是否转换过开关
    private boolean transformed = false;

    // 转换过后的 SQL
    private ByteString transformedSql = null;

    private List<DoubleQuoteIdentifier> doubleQuoteIdentifiers = Lists.newArrayList();

    private static final char DOUBLE_QUOTE_CHAR = '"';

    private static final char BACK_QUOTE_CHAR = '`';

    public MySQLANSIQuoteTransformer(ByteString sql) throws SQLSyntaxErrorException {
        super(sql);
        this.isJumpHint = true;
//        scanChar();
//        wrappedNextToken();
    }

    /**
     * 获取转换过后的 SQL
     */
    public ByteString getTransformerdSql() throws SQLSyntaxErrorException {
        // 延迟转换直到客户端请求发来
        if (!transformed) {
            transform();
            transformed = true;
        }

        return transformedSql;
    }

    /**
     * 遍历整个 sql 识别出需要替换的双引号标记的identifier
     * @throws SQLSyntaxErrorException
     */
    private void transform() throws SQLSyntaxErrorException {
        // 遍历整个 SQL
        scanSql();

        // 开始转换
        ByteBuffer buffer = ByteBuffer.allocate(sql.length());
        int dqiIndex = 0;
        DoubleQuoteIdentifier nextDQI = getDoubleQuoteIdentifier(dqiIndex);
        int sqlIndex = 0;
        while (nextDQI != null && sqlIndex < sql.length()) {
            if (nextDQI.indexBeforeScope(sqlIndex)) {
                buffer.put((byte) charAt(sqlIndex++));
            } else if (nextDQI.indexInScope(sqlIndex)) {
                buffer.put(nextDQI.getSubstitution());
                sqlIndex = nextDQI.getEndIndex();
                nextDQI = getDoubleQuoteIdentifier(++dqiIndex);
            }
        }

        while (sqlIndex < sql.length()) {
            char _ch = charAt(sqlIndex++);
            if (_ch != EOI) {
                buffer.put((byte) _ch);
            }
        }

        transformedSql = new ByteString(buffer.array(), 0, buffer.position(), sql.getCharset());
    }

    /**
     * 遍历整个 SQL
     *
     * @throws SQLSyntaxErrorException
     */
    private void scanSql() throws SQLSyntaxErrorException {
        scanChar();
        wrappedNextToken();

        while (!eof()) {
            skipSeparator();
            wrappedNextToken();
        }
    }

    /**
     * 获取doubleQuoteIdentifiers中给定位置处的DoubleQuoteIdentifier
     *
     * @param dqiIndex 给定位置
     * @return 存在则返回 doubleQuoteIdentifiers.get(dqiIndex)，不存在返回 null
     */
    private DoubleQuoteIdentifier getDoubleQuoteIdentifier(int dqiIndex) {
        return dqiIndex >= doubleQuoteIdentifiers.size() ? null : doubleQuoteIdentifiers.get(dqiIndex);
    }

    /**
     * 扩充nextToken函数，加入预处理语义：识别出由double quote标记的identifier。
     *
     * @throws SQLSyntaxErrorException
     */
    private void wrappedNextToken() throws SQLSyntaxErrorException {
        switch(ch) {
            case '\"':
                scanDoubleQuoteIdentifier();
                break;
            default:
                nextToken();
        }
    }

    /**
     * 识别sql中一段由双引号标记的identifier，需要将该identifier改为反引号的形式。
     *
     * 算法逻辑：
     *   将起始双引号被替换为反引号，该 identifier 内部的转义双引号（由两个双引号组成）被替换为单双引号，
     *   反引号被替换转义反引号（由两个反引号组成）。
     *
     */
    private void scanDoubleQuoteIdentifier() {
        ByteArrayOutputStream substitution = new ByteArrayOutputStream();
        substitution.write(BACK_QUOTE_CHAR);
        int startIndex = curIndex;
        while (scanChar() != MySQLLexer.EOI) {
            if (ch != DOUBLE_QUOTE_CHAR) {
                if (ch == BACK_QUOTE_CHAR) {
                    // 反引号被替换转义反引号（由两个反引号组成）
                    substitution.write(BACK_QUOTE_CHAR);
                    substitution.write(BACK_QUOTE_CHAR);
                } else {
                    substitution.write(ch);
                }
            } else {
                if (scanChar() != DOUBLE_QUOTE_CHAR) {
                    break;
                } else {
                    // 转义双引号（由两个双引号组成）被替换为单双引号
                    substitution.write(DOUBLE_QUOTE_CHAR);
                }
            }
        }
        int endIndex = curIndex;
        substitution.write(BACK_QUOTE_CHAR);
        doubleQuoteIdentifiers.add(new DoubleQuoteIdentifier(substitution.toByteArray(), startIndex, endIndex));
    }
}

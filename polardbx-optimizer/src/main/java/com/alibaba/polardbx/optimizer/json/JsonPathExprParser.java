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

package com.alibaba.polardbx.optimizer.json;

import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLLexer;
import com.alibaba.polardbx.optimizer.parse.mysql.lexer.MySQLToken;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.SQLSyntaxErrorException;
import java.util.List;
import java.util.Map;

/**
 *
 * pathExpression:
 *     scope[(pathLeg)*]
 *
 * pathLeg:
 *     member | arrayLocation | doubleAsterisk
 *
 * member:
 *     period ( keyName | asterisk )
 *
 * arrayLocation:
 *     leftBracket ( nonNegativeInteger | asterisk ) rightBracket
 *
 * keyName:
 *     ESIdentifier | doubleQuotedString
 *
 * doubleAsterisk:
 *     '**'
 *
 * period:
 *     '.'
 *
 * asterisk:
 *     '*'
 *
 * leftBracket:
 *     '['
 *
 * rightBracket:
 *     ']'
 *
 * @author arnkore 2017-07-12 16:33
 */
public class JsonPathExprParser {

    private MySQLLexer lexer;

    private String expressionSql;

    private enum SpecialIdentifier {
        /* '$' */
        DOLLAR,
        /* '*' */
        ASTERISK,
    }

    private static final Map<String, SpecialIdentifier> specialIdentifierMap = Maps.newHashMap();
    static {
        specialIdentifierMap.put("$", SpecialIdentifier.DOLLAR);
        specialIdentifierMap.put("*", SpecialIdentifier.ASTERISK);
    }

    public JsonPathExprParser(MySQLLexer lexer) {
        this.lexer = lexer;
        expressionSql = lexer.getSQL();
    }

    public JsonPathExprStatement parse() throws SQLSyntaxErrorException {
        List<AbstractPathLeg> pathLegs = Lists.newArrayList();
        // process scope
        String str = lexer.stringValue();
        SpecialIdentifier si = specialIdentifierMap.get(str);
        if (si != null && si == SpecialIdentifier.DOLLAR) {
            lexer.nextToken();
        } else {
            err(String.format("Illegal path expression: %s, expect $ letter at begining", expressionSql));
        }

        // process pathleg
        boolean isPathLeg = true;
        while (isPathLeg) {
            if (lexer.token() == MySQLToken.PUNC_DOT) { // member
                lexer.nextToken();
                String keyName = null;
                // keyName or asterisk
                if (lexer.token() != MySQLToken.OP_ASTERISK) {
                    keyName = lexer.stringValue();
                }
                lexer.nextToken();
                pathLegs.add(new Member(keyName));
            } else if (lexer.token() == MySQLToken.PUNC_LEFT_BRACKET) { // arrayLocation
                lexer.nextToken();
                Integer index = null;
                // non-negative integer or asterisk
                if (lexer.token() != MySQLToken.OP_ASTERISK) {
                    index = lexer.integerValue().intValue();
                }
                lexer.nextToken();
                match(MySQLToken.PUNC_RIGHT_BRACKET);
                pathLegs.add(new ArrayLocation(index));
            } else if (lexer.token() == MySQLToken.OP_ASTERISK) { // double asterisk
                match(MySQLToken.OP_ASTERISK);
                match(MySQLToken.OP_ASTERISK);
                pathLegs.add(new DoubleAsterisk());
            } else {
                isPathLeg = false;
            }
        }

        return new JsonPathExprStatement(pathLegs);
    }

    private int match(MySQLToken... expectToken) throws SQLSyntaxErrorException {
        if (expectToken == null || expectToken.length <= 0) throw new IllegalArgumentException("at least one expect token");
        MySQLToken token = lexer.token();
        for (int i = 0; i < expectToken.length; ++i) {
            if (token == expectToken[i]) {
                if (token != MySQLToken.EOF || i < expectToken.length - 1) {
                    lexer.nextToken();
                }
                return i;
            }
        }
        throw err("expect " + expectToken);
    }

    private SQLSyntaxErrorException err(String msg) throws SQLSyntaxErrorException {
        StringBuilder errmsg = new StringBuilder();
        errmsg.append(msg).append(". lexer state: ").append(String.valueOf(lexer));
        throw new SQLSyntaxErrorException(errmsg.toString());
    }
}

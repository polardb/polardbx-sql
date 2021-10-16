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

package com.alibaba.polardbx.druid.sql.dialect.mysql.parser;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.util.JdbcUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;

public class MySqlInsertReader implements Closeable {
    private final Reader in;

    private char[] buf = new char[1024];
    private int pos;
    private char ch;

    MySqlStatementParser parser;

    private MySqlInsertStatement statement;

    public MySqlInsertReader(Reader in) {
        this.in = in;

    }

    public MySqlInsertStatement parseStatement() throws IOException {
        in.read(buf);
        String text = new String(buf);
        parser = new MySqlStatementParser(ByteString.from(text), SQLParserFeature.InsertReader);

        statement = (MySqlInsertStatement) parser.parseStatement();
        this.pos = parser.getLexer().pos() - 1;
        this.ch = buf[pos];

        return statement;
    }

    public MySqlInsertStatement getStatement() {
        return statement;
    }

    public SQLInsertStatement.ValuesClause readCaluse() {
        return null;
    }

    public boolean isEOF() {
        return false;
    }

    @Override
    public void close() {
        JdbcUtils.close(in);
    }
}

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

import com.alibaba.polardbx.optimizer.core.rel.SqlNodeCloneVisitor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author lingce.ldm 2018-02-02 15:55
 */
@Ignore
public class SqlNodeCloneVisitorTest {

    @Ignore
    public void cloneSelectTest() throws SqlParseException {
        String sql = "Select * from t where id = 2";
        SqlSelect select = (SqlSelect) parse(sql);
        SqlNodeCloneVisitor visitor = new SqlNodeCloneVisitor();
        SqlSelect clone = (SqlSelect) select.accept(visitor);

        /**
         * Now modify it.
         */
//        SqlIdentifier identifier = new SqlIdentifier("name", SqlParserPos.ZERO);
        SqlLiteral literal = SqlCharStringLiteral.createCharString("origin", SqlParserPos.ZERO);
//        select.setWhere(SqlStdOperatorTable.EQUALS.createCall(SqlParserPos.ZERO, identifier, literal));

        SqlNode where = select.getWhere();
        SqlCall call = (SqlCall) where;
        call.setOperand(1, literal);

        System.out.println(select);
        System.out.println(clone);
    }

    private SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql);
        return parser.parseQuery();
    }

}

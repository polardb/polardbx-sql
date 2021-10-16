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

package com.alibaba.polardbx.druid.bvt.sql.mysql.delete;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import org.junit.Test;

import java.util.List;

/**
 * Description:
 *
 * @author: yuyang.cjx
 * @date: 2019-03-01
 */
public class MysqlDeleteTest {

    @Test
    public void testDelete() {


        String sql = "delete from xxx where id = 1 and id = 2 or id = 3 ";

        parse(sql);

        sql = "delete from xxx a";

        parse(sql);

        sql = "delete from xxx a where id = 1 or xxx = 'xxx'";

        parse(sql);

        sql = "delete from xxx a   ";

        parse(sql);

        sql = "delete from xxx a    /** commoent **/";

        parse(sql);

        sql = "delete from xxx a   ; /*** comment ***/ ";

        parse(sql);

    }

    @Test
    public void testDeleteLimit() {

        String sql = "delete from xxx where id = 1 order by id desc limit 1";

        parse(sql);

    }

    @Test(expected = ParserException.class)
    public void testDeleteSetMustError() {

        String sql = "delete from xxx set id = 1";
        parse(sql);



    }

    @Test(expected = ParserException.class)
    public void testDeleteError() {

        String sql = "delete from xxx where 1 = 1 lala 1";

        parse(sql);

    }

    private List<SQLStatement> parse(String sql) {
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        return parser.parseStatementList();
    }

}

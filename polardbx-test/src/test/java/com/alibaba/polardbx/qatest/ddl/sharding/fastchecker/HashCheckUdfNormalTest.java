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

package com.alibaba.polardbx.qatest.ddl.sharding.fastchecker;

/**
 * Created by zhuqiwei.
 *
 * @author: zhuqiwei
 */
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.text.MessageFormat;

@Ignore
public class HashCheckUdfNormalTest extends AsyncDDLBaseNewDBTestCase {

    private static final String TABLE_NAME = "hash_check_table";
    private static final String TABLE_TEMPLATE = "create table {0} (\n"
            + "    pk bigint not null,\n"
            + "    col1 int default null,\n"
            + "    col2 varchar(50) default null,\n"
            + "    col3 varchar(50) default null,\n"
            + "    primary key(pk)\n"
            + ")";

    private static final String INSERT_TEMPLATE = "insert into {0} (pk, col1, col2, col3) values "
            + "(1, 1000, \"stringpadding0\", \"stringpadding1234\"),"
            + "(2, null, null, null),"
            + "(3, 1001, \"\", null),"
            + "(4, 1002, \"\", \"\"),"
            + "(5, 1003, \"abc\", \"de\"),"
            + "(6, 1004, \"ab\", \"cde\")";

    private final static String[] simpleTestTemplates = {
            "select hashcheck(pk, col1, col2, col3) from {0}",
            "select hashcheck(pk, col1, col2, col3) from {0} order by pk desc",
            "select hashcheck(pk, col1, col2, col3) from {0} where pk = 1",
            "select hashcheck(pk, col1, col2, col3) from {0} where pk = 2",
            "select hashcheck(pk, col1, col2, col3) from {0} where pk = 3",
            "select hashcheck(pk, col1, col2, col3) from {0} where pk = 4",
            "select hashcheck(pk, col1, col2, col3) from {0} where pk <= 2",
            "select hashcheck(col1, col2, col3) from {0} where pk = 2",
            "select hashcheck(col2, col3) from {0} where pk = 2",
            "select hashcheck(col2, col3) from {0} where pk = 3",
            "select hashcheck(col2, col3) from {0} where pk = 4",
            "select hashcheck(col2, col3) from {0} where pk = 5",
            "select hashcheck(col2, col3) from {0} where pk = 6",
            "select hashcheck(col2, col3) from {0} where false",
    };

    private final static Long[] simpleTestAnswers = {
            -8659724057413361349L,
            -8659724057413361349L,
            -3716545088072518871L,
            3960408347517713012L,
            -2315392276716221894L,
            -639074828579739066L,
            -3247547720101282122L,
            -4714742188124545047L,
            5375440410188756893L,
            -5835211101738819338L,
            65535L,
            -8531408785684950115L,
            2581744308424875983L,
            null
    };

    private final static String wrongArgSelect = "select hashcheck() from {0}";

    @Before
    public void initData() {
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
                "drop table if exists " + quoteSpecialName(TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
                MessageFormat.format(TABLE_TEMPLATE, quoteSpecialName(TABLE_NAME)));

        JdbcUtil.executeUpdateSuccess(mysqlConnection,
                MessageFormat.format(INSERT_TEMPLATE, quoteSpecialName(TABLE_NAME)));

    }

    @Test
    public void testNormalHashcheck() {
        for (int i = 0; i < simpleTestTemplates.length; i++) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(mysqlConnection,
                    MessageFormat.format(simpleTestTemplates[i], quoteSpecialName(TABLE_NAME)));
            Long ans = JdbcUtil.resultLong(rs);
            JdbcUtil.close(rs);
            Assert.assertTrue(simpleTestAnswers[i] != null && simpleTestAnswers[i].equals(ans) || simpleTestAnswers[i] == null && ans == null);
        }
    }

    /**
     * hashcheck UDF will return error if the input doesn't have argument
     */
    @Test
    public void testErrorCase() {
        JdbcUtil.executeQueryFaied(mysqlConnection,
                MessageFormat.format(wrongArgSelect, quoteSpecialName(TABLE_NAME)),
                "wrong number of arguments");
    }

    @After
    public void cleanup() {
        JdbcUtil.executeUpdateSuccess(mysqlConnection,
                "drop table if exists " + quoteSpecialName(TABLE_NAME));
    }

}


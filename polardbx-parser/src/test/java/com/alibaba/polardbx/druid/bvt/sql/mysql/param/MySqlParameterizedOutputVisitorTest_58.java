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

package com.alibaba.polardbx.druid.bvt.sql.mysql.param;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_58 extends TestCase {
    final DbType dbType = JdbcConstants.MYSQL;
    public void test_for_parameterize() throws Exception {

        String sql = "select * from t where id = 101";

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType, params);
        assertEquals("SELECT *\n" +
                "FROM t\n" +
                "WHERE id = ?", psql);
        assertEquals(1, params.size());
        assertEquals(101, params.get(0));
    }

    public void test_for_parameterize_insert() throws Exception {
        String sql = "insert into mytab(fid,fname)values(1001,'wenshao');";

        List<Object> params = new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, dbType, params);
        assertEquals("INSERT INTO mytab(fid, fname)\n" +
                "VALUES (?, ?);", psql);
        assertEquals(2, params.size());
        assertEquals(1001, params.get(0));
        assertEquals("wenshao", params.get(1));
    }
}

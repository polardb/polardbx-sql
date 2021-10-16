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

package com.alibaba.polardbx.druid.bvt.transform;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.transform.SQLUnifiedUtils;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import junit.framework.TestCase;

/**
 * @author lijun.cailj 2020/5/8
 */
public class SQLUnifiedVisitorTest extends TestCase {

    public void test() throws Exception {
        String sql1 = "select ttt.b, ttt.a, ttt.c as a from t as ttt where a and b  or d and c group by df,e,d, 1+1 ";
        String sql2 = "select ttt.a, ttt.c, ttt.b as a from t as ttt where b and a  or d and c group by e,d, df, 1+1 ";
        equalsSQL(sql1, sql2);
        equalsHash(sql1, sql2);
        equalsParameterized(sql1, sql2);
    }

    public void test2() throws Exception {
        String sql1 = "select tt.b, tt.a, tt.c as a from t as tt where a and b  and d and c group by df,e,d, 1+1 ";
        String sql2 = "select tt.a, tt.c, tt.b as a from t as tt where b and a  and d and c group by e,d, df, 1+1 ";

        equalsSQL(sql1, sql2);
        equalsHash(sql1, sql2);
        equalsParameterized(sql1, sql2);
    }

    public void test3() throws Exception {
        String sql1 = "select tt.b, tt.a, tt.c as a from t as tt where a =1 and f =2 and d = 3 group by df,e,d, 1+1 ";
        String sql2 = "select tt.a, tt.c, tt.b as a from t as tt where a = 3 and d = 3  and f=3 group by e,d, df, 1+1 ";

        equalsSQL(sql1, sql2);
        equalsHash(sql1, sql2);
        equalsParameterized(sql1, sql2);
    }

    public void test4() throws Exception {
        String sql1 = "insert into table1 select tt.b, tt.a, tt.c as a from t as tt where a =1 and f =2 and d = 3 group by df,e,d, 1+1 ";
        String sql2 = "insert into table1 select tt.a, tt.c, tt.b as a from t as tt where a = 3 and d = 3  and f=3 group by e,d, df, 1+1 ";

        equalsSQL(sql1, sql2);
    }

    public void test5() throws Exception {
        String sql1 = "select s.col1 from a S where S.col2>1 and S.col1 > 2";

        String sql2 = "select s.col1 from a S where S.col2>2 and S.col1 > 3";

        equalsSQL(sql1, sql2);
    }

    public void test6() throws Exception {
        String sql1 = "select s.a from (select t.a from t s) s where s.a > 1 ";

        String sql2 = "select s.a from (select t.a from t s) s where s.a > 3";

        equalsSQL(sql1, sql2);
    }
/*

    public void test_union() throws Exception {
        String sql1 = "select tt.b, tt.a, tt.c as a from t as tt where a and b  and d and c group by df,e,d, 1+1 " +
                " union select a,c,d  from t2" +
                " union select d,d,d from t1";

        String sql2 =
                " select tt.a, tt.c, tt.b as a from t as tt where b and a  and d and c group by e,d, df, 1+1 " +
                " union select d,d,d from t1 "+
                " union select a,c,d  from t2";

        equalsSQL(sql1, sql2);
        equalsHash(sql1, sql2);
    }
*/

    public void equalsSQL(String sql1, String sql2) {
        String newSQL1 = SQLUnifiedUtils.unifySQL(sql1, DbType.mysql);
        String newSQL2 = SQLUnifiedUtils.unifySQL(sql2, DbType.mysql);

        System.out.println(newSQL1);
        System.out.println();
        System.out.println(newSQL2);
        assertEquals(newSQL1, newSQL2);
    }

    public void equalsHash(String sql1, String sql2) {
        long hash1 = SQLUnifiedUtils.unifyHash(sql1, DbType.mysql);
        long hash2 = SQLUnifiedUtils.unifyHash(sql2, DbType.mysql);

        assertEquals(hash1, hash2);
    }

    public void equalsParameterized(String sql1, String sql2) {
        String newSQL1 = SQLUnifiedUtils.unifySQL(sql1, DbType.mysql);
        String newSQL2 = SQLUnifiedUtils.unifySQL(sql2, DbType.mysql);

        String parameterize1 = ParameterizedOutputVisitorUtils.parameterize(newSQL1, DbType.mysql);
        String parameterize2 = ParameterizedOutputVisitorUtils.parameterize(newSQL2, DbType.mysql);

        assertEquals(parameterize1, parameterize2);
    }
}
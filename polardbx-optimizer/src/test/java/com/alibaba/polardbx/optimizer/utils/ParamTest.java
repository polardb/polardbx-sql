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

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLSelectListCache;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.SerializeUtils;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

/**
 * @author lingce.ldm 2017-12-06 16:48
 */
public class ParamTest {

    @Test
    public void test_for_mysql_param() throws Exception {
        DbType dbType = JdbcConstants.MYSQL;
        String sql =
            "/* 0b853c4a26094480140194289e3d24/0.1.1.2.1//2e3b9cf7/ */select `miller_cart`.`CART_ID`,`miller_cart`.`SKU_ID`,`miller_cart`.`ITEM_ID`,`miller_cart`.`QUANTITY`,`miller_cart`.`USER_ID`,`miller_cart`.`SELLER_ID`,`miller_cart`.`STATUS`,`miller_cart`.`EXT_STATUS`,`miller_cart`.`TYPE`,`miller_cart`.`SUB_TYPE`,`miller_cart`.`GMT_CREATE`,`miller_cart`.`GMT_MODIFIED`,`miller_cart`.`ATTRIBUTE`,`miller_cart`.`ATTRIBUTE_CC`,`miller_cart`.`EX2` from `miller_cart_0304` `miller_cart` where ((`miller_cart`.`USER_ID` = 2732851504) AND ((`miller_cart`.`STATUS` = 1) AND (`miller_cart`.`TYPE` IN (0,5,10)))) limit 0,200";
        long hash1 = ParameterizedOutputVisitorUtils.parameterizeHash(sql, dbType, null, null);
        long hash2 = FnvHash.fnv1a_64_lower(ParameterizedOutputVisitorUtils.parameterize(sql, dbType));
        assertEquals(hash1, hash2);

        SQLSelectListCache cache = new SQLSelectListCache(dbType);
        cache.add(
            "select `miller_cart`.`CART_ID`,`miller_cart`.`SKU_ID`,`miller_cart`.`ITEM_ID`,`miller_cart`.`QUANTITY`,`miller_cart`.`USER_ID`,`miller_cart`.`SELLER_ID`,`miller_cart`.`STATUS`,`miller_cart`.`EXT_STATUS`,`miller_cart`.`TYPE`,`miller_cart`.`SUB_TYPE`,`miller_cart`.`GMT_CREATE`,`miller_cart`.`GMT_MODIFIED`,`miller_cart`.`ATTRIBUTE`,`miller_cart`.`ATTRIBUTE_CC`,`miller_cart`.`EX2` from");

        List<Object> outParameters = new ArrayList<>(); // 这个参数如果为null时，性能会进一步提升
        long hash3 = ParameterizedOutputVisitorUtils.parameterizeHash(sql, dbType, cache, outParameters);
        assertEquals(hash1, hash3);
    }

    @Test
    public void testFastSqlParser() {
        String sql = "select * from t1 where id > 1; select id from t2 where name like '%lc'";
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        List<SQLStatement> stmtList = parser.parseStatementList();
        for (SQLStatement s : stmtList) {
            System.out.println(s.toString());
        }

        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(sql);
        System.out.println(sqlParameterized.toString());
    }

    @Test
    public void testMppParams() throws Exception {

        Map<Integer, ParameterContext> currentParameter = new HashMap<>();

        Object[] obj1 = new Object[2];
        obj1[0] = new BigDecimal("123.566653");
        obj1[1] = new byte[] {111, 10, 34, 32, 34, 55, 110};

        currentParameter.put(1, new ParameterContext(ParameterMethod.setObject1, obj1));

        Parameters p1 = new Parameters(currentParameter);

        Parameters p2 = SerializeUtils.deFromBytes(SerializeUtils.getBytes(p1), Parameters.class);

        assertTrue(p2.getCurrentParameter().get(1).getArgs()[0] instanceof BigDecimal);
        assertTrue(p2.getCurrentParameter().get(1).getArgs()[1] instanceof byte[]);

        assertEquals(obj1[0], p2.getCurrentParameter().get(1).getArgs()[0]);

        assertTrue(Arrays.equals((byte[]) obj1[1], (byte[]) p2.getCurrentParameter().get(1).getArgs()[1]));
    }

}

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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 16/9/23.
 */
public class MySqlParameterizedOutputVisitorTest_61 extends TestCase {
    public void test_for_parameterize() throws Exception {

        String sql = "insert ignore into ktv_ibx_1690 (id, msg_id, cid, openid, gmt_create, gmt_modified, read_status, reach_status, create_at, type, tag, sender_id, extension, domain, at_me) " +
                "VALUES " +
                "(399644333, 49099001633, \"543351210\", 306202, now(), now(), 2, 2, 1515051603113, 51, 0, 26659032, '', 'mingguo', 0), " +
                "(399644599334, 49399311994, \"543351210\", 306202, now(), now(), 2, 2, 1515045263923, 51, 0, 55235569, '', 'mingguo', 0), " +
                "(399644599335, 49352513493, \"543351210\", 306202, now(), now(), 2, 2, 1515032652532, 51, 0, 91605449, '', 'mingguo', 0)";

        List<Object> params =  new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params);
        assertEquals("INSERT IGNORE INTO ktv_ibx (id, msg_id, cid, openid, gmt_create\n" +
                "\t, gmt_modified, read_status, reach_status, create_at, type\n" +
                "\t, tag, sender_id, extension, domain, at_me)\n" +
                "VALUES (?, ?, ?, ?, now()\n" +
                "\t\t, now(), ?, ?, ?, ?\n" +
                "\t\t, ?, ?, ?, ?, ?)", psql);
        assertEquals(3, params.size());
        assertEquals("[399644333,49099001633,\"543351210\",306202,2,2,1515051603113,51,0,26659032,\"\",\"mingguo\",0]", JSON.toJSONString(params.get(0)));
        assertEquals("[399644599334,49399311994,\"543351210\",306202,2,2,1515045263923,51,0,55235569,\"\",\"mingguo\",0]", JSON.toJSONString(params.get(1)));
        assertEquals("[399644599335,49352513493,\"543351210\",306202,2,2,1515032652532,51,0,91605449,\"\",\"mingguo\",0]", JSON.toJSONString(params.get(2)));

        String rsql = ParameterizedOutputVisitorUtils.restore(sql, JdbcConstants.MYSQL, params);
        assertEquals("INSERT IGNORE INTO ktv_ibx_1690 (id, msg_id, cid, openid, gmt_create\n" +
                "\t, gmt_modified, read_status, reach_status, create_at, type\n" +
                "\t, tag, sender_id, extension, domain, at_me)\n" +
                "VALUES (399644333, 49099001633, '543351210', 306202, now()\n" +
                "\t\t, now(), 2, 2, 1515051603113, 51\n" +
                "\t\t, 0, 26659032, '', 'mingguo', 0),\n" +
                "\t(399644599334, 49399311994, '543351210', 306202, now()\n" +
                "\t\t, now(), 2, 2, 1515045263923, 51\n" +
                "\t\t, 0, 55235569, '', 'mingguo', 0),\n" +
                "\t(399644599335, 49352513493, '543351210', 306202, now()\n" +
                "\t\t, now(), 2, 2, 1515032652532, 51\n" +
                "\t\t, 0, 91605449, '', 'mingguo', 0)", rsql);
    }

}

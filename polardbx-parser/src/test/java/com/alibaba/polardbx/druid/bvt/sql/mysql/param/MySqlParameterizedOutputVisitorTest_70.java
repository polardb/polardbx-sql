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
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class MySqlParameterizedOutputVisitorTest_70 extends TestCase {
    public void test_in() throws Exception {

        String sql = "select ((0='x6') & 31) ^ (ROW(76, 4) NOT IN (ROW(1, 2 ),ROW(3, 4)) );";

        List<Object> params =  new ArrayList<Object>();
        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, params
                , VisitorFeature.OutputParameterizedUnMergeShardingTable
                ,VisitorFeature.OutputParameterizedQuesUnMergeInList
        );
        assertEquals("SELECT ((? = ?) & ?) ^ (ROW(?, ?) NOT IN (ROW(?, ?), ROW(?, ?)));", psql);
        assertEquals(9, params.size());
        assertEquals("0", JSON.toJSONString(params.get(0)));

        assertEquals("[0,\"x6\",31,76,4,1,2,3,4]", JSON.toJSONString(params));

        String rsql = ParameterizedOutputVisitorUtils.restore(psql, JdbcConstants.MYSQL, params);
        assertEquals("SELECT ((0 = 'x6') & 31) ^ (ROW(76, 4) NOT IN (ROW(1, 2), ROW(3, 4)));", rsql);
    }


}

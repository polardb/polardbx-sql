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
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 16/8/23.
 */
public class MySqlParameterizedOutputVisitorTest_78_nchar extends TestCase {

    public void test_for_parameterize() throws Exception {
        String sql = "select N'1' as `customerid`,N'5004' as `ordersourceid`,'2018-12-13 21:15:30.879' as `creationtime`";
        List<Object> outParameters = new ArrayList<Object>(0);

        String psql = ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParameters, VisitorFeature.OutputParameterizedQuesUnMergeOr);
        assertEquals("SELECT ? AS `customerid`, ? AS `ordersourceid`\n\t, ? AS `creationtime`", psql);

        assertEquals("[\"1\",\"5004\",\"2018-12-13 21:15:30.879\"]", JSON.toJSONString(outParameters));

        String rsql = ParameterizedOutputVisitorUtils.restore(sql, DbType.mysql, outParameters);
        assertEquals("SELECT N'1' AS `customerid`, N'5004' AS `ordersourceid`\n"
            + "\t, '2018-12-13 21:15:30.879' AS `creationtime`", rsql);
    }
}

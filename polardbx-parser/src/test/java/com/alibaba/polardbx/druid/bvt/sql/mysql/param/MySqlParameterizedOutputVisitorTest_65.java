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

import com.alibaba.polardbx.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wenshao on 16/9/23.
 */
public class MySqlParameterizedOutputVisitorTest_65 extends TestCase {
    public void test_for_parameterize() throws Exception {
        List<Object> outParams =  new ArrayList<Object>();

        String sql = "select * from abc";
        assertEquals("select * from abc", ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL));
        assertEquals("SELECT *\n" +
                "FROM abc", ParameterizedOutputVisitorUtils.parameterize(sql, JdbcConstants.MYSQL, outParams, VisitorFeature.OutputParameterizedZeroReplaceNotUseOriginalSql));

    }

}

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

package com.alibaba.polardbx.druid.bvt.sql.eval;

import com.alibaba.polardbx.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.junit.Assert;


public class EvalTest_multi_short extends TestCase {
    public void test_byte() throws Exception {
        Assert.assertEquals(2, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? * ?", (short) 1, (byte) 2));
    }
    
    public void test_byte_1() throws Exception {
        Assert.assertEquals(2, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? * ?", (short) 1, "2"));
    }
    
    public void test_byte_2() throws Exception {
        Assert.assertEquals(null, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? * ?", (short) 1, null));
    }
    
    public void test_byte_3() throws Exception {
        Assert.assertEquals(2, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? * ?", "2", (short) 1));
    }
}

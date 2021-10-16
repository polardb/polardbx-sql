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


public class EvalTest_add_long extends TestCase {
    public void test_add() throws Exception {
        Assert.assertEquals(3L, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? + ?", (long) 1, (byte) 2));
    }
    
    public void test_add_1() throws Exception {
        Assert.assertEquals(3L, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? + ?", (long) 1, "2"));
    }
    
    public void test_add_2() throws Exception {
        Assert.assertEquals(null, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? + ?", (long) 1, null));
    }
    
    public void test_add_3() throws Exception {
        Assert.assertEquals(3L, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? + ?", (byte) 2, (long) 1));
    }
    
    public void test_add_4() throws Exception {
        Assert.assertEquals(3L, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? + ?", "2", (long) 1));
    }
    
    public void test_add_5() throws Exception {
        Assert.assertEquals(null, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? + ?", null, (long) 1));
    }
}

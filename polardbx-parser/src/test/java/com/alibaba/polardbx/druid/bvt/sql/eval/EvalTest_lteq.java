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

import java.math.BigDecimal;
import java.math.BigInteger;


public class EvalTest_lteq extends TestCase {
    public void test_long() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? <= ?", (long) 1, (byte) 2));
    }
    
    public void test_int() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? <= ?", (int) 1, (byte) 2));
    }
    
    public void test_short() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? <= ?", (short) 1, (byte) 2));
    }
    
    public void test_byte() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "? <= ?", (byte) 1, (byte) 2));
    }
    
    public void test_BigInteger() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "?<=?", BigInteger.ONE, (byte) 2));
    }
    
    public void test_BigDecimal() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "?<=?", BigDecimal.ONE, (byte) 2));
    }
    
    public void test_float() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "?<=?", (float) 1, (byte) 2));
    }
    
    public void test_double() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "?<=?", (double) 1, (byte) 2));
    }
    
    public void test_String() throws Exception {
        Assert.assertEquals(true, SQLEvalVisitorUtils.evalExpr(JdbcConstants.MYSQL, "?<=?", "1", "2"));
    }
}

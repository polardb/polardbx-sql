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

package com.alibaba.polardbx.druid.sql.parser;

import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SQLStatementParserTest
{
    @Test
    public void testInsertFunction()
            throws SQLException
    {
        MySqlStatementParser sqlStatementParser = new MySqlStatementParser("insert into batch_features_pb (id, url, feature) values(0, 'https://cbu01.alicdn.com/img/ibank/2013/441/486/1036684144_687583347.jpg', CLOTHES_FEATURE_EXTRACT_V1(url))");
        sqlStatementParser.config(SQLParserFeature.InsertReader, true);

        MyInsertValueHandler myInsertValueHandler = new MyInsertValueHandler();
        sqlStatementParser.parseInsert();
        sqlStatementParser.parseValueClause(myInsertValueHandler);
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertEquals("url",
                myInsertValueHandler.getFunctions().get("CLOTHES_FEATURE_EXTRACT_V1"));
    }

    @Test
    public void testInsertFunction2()
            throws SQLException
    {
        MySqlStatementParser sqlStatementParser = new MySqlStatementParser("insert into batch_features_pb (id, url, feature) values(0, \"https://cbu01.alicdn.com/img/ibank/2013/441/486/1036684144_687583347.jpg\", CLOTHES_FEATURE_EXTRACT_V1(url))");
        sqlStatementParser.config(SQLParserFeature.InsertReader, true);

        MyInsertValueHandler myInsertValueHandler = new MyInsertValueHandler();
        sqlStatementParser.parseInsert();
        sqlStatementParser.parseValueClause(myInsertValueHandler);
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertEquals("url",
                myInsertValueHandler.getFunctions().get("CLOTHES_FEATURE_EXTRACT_V1"));
    }

    @Test
    public void testInsertFunctions()
            throws SQLException
    {
        MySqlStatementParser sqlStatementParser = new MySqlStatementParser("insert into batch_features_pb (id, url, feature) values(0, 'https://cbu01.alicdn.com/img/ibank/2013/441/486/1036684144_687583347.jpg', CLOTHES_FEATURE_EXTRACT_V1(url), CLOTHES_ATTRIBUTE_EXTRACT_V1(url))");
        sqlStatementParser.config(SQLParserFeature.InsertReader, true);

        MyInsertValueHandler myInsertValueHandler = new MyInsertValueHandler();
        sqlStatementParser.parseInsert();
        sqlStatementParser.parseValueClause(myInsertValueHandler);
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertTrue(myInsertValueHandler.getFunctions().containsKey("CLOTHES_ATTRIBUTE_EXTRACT_V1"));
        Assert.assertEquals("url",
                myInsertValueHandler.getFunctions().get("CLOTHES_FEATURE_EXTRACT_V1"));
        Assert.assertEquals("url",
                myInsertValueHandler.getFunctions().get("CLOTHES_ATTRIBUTE_EXTRACT_V1"));
    }

    private class MyInsertValueHandler
            implements SQLInsertValueHandler
    {
        private Map<String, Object> functions = new HashMap<String, Object>();

        public Map<String, Object> getFunctions()
        {
            return functions;
        }

        @Override
        public Object newRow()
                throws SQLException
        {
            return null;
        }

        @Override
        public void processInteger(Object row, int index, Number value)
                throws SQLException
        {

        }

        @Override
        public void processString(Object row, int index, String value)
                throws SQLException
        {

        }

        @Override
        public void processDate(Object row, int index, String value)
                throws SQLException
        {

        }

        @Override
        public void processDate(Object row, int index, Date value)
                throws SQLException
        {

        }

        @Override
        public void processTimestamp(Object row, int index, String value)
                throws SQLException
        {

        }

        @Override
        public void processTimestamp(Object row, int index, Date value)
                throws SQLException
        {

        }

        @Override
        public void processTime(Object row, int index, String value)
                throws SQLException
        {

        }

        @Override
        public void processDecimal(Object row, int index, BigDecimal value)
                throws SQLException
        {

        }

        @Override
        public void processBoolean(Object row, int index, boolean value)
                throws SQLException
        {

        }

        @Override
        public void processNull(Object row, int index)
                throws SQLException
        {

        }

        @Override
        public void processFunction(Object row, int index, String funcName, long funcNameHashCode64, Object... values)
                throws SQLException
        {
            this.functions.put(funcName, values[0]);
        }

        @Override
        public void processRow(Object row)
                throws SQLException
        {

        }

        @Override
        public void processComplete()
                throws SQLException
        {

        }
    }
}
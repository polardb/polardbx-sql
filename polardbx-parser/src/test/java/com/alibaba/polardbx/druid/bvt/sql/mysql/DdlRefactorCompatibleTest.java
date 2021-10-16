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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import junit.framework.TestCase;
import org.junit.Assert;

import java.util.List;

/**
 * @version 1.0
 * @ClassName DdlRefactorCompatibleTest
 * @description
 * @Author zzy
 * @Date 2019-07-26 09:50
 */
public class DdlRefactorCompatibleTest extends TestCase {

    public void test_0() {
        String sql = "alter table tb add index (a) KEY_BLOCK_SIZE=32 using hash comment 'hehe' DistanceMeasure = DotProduct ALGORITHM = IVF";
        SQLStatementParser parser = new MySqlStatementParser(sql,
                SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);

        Assert.assertTrue(result instanceof SQLAlterTableStatement);
        SQLAlterTableStatement statement = (SQLAlterTableStatement) result;
        Assert.assertEquals(1, statement.getItems().size());
        Assert.assertTrue(statement.getItems().get(0) instanceof SQLAlterTableAddIndex);
        SQLAlterTableAddIndex addIndex = (SQLAlterTableAddIndex) statement.getItems().get(0);

        Assert.assertEquals("DotProduct", addIndex.getDistanceMeasure());
        Assert.assertEquals("IVF", addIndex.getAlgorithm());
        Assert.assertEquals(3, addIndex.getOptions().size());
        Assert.assertTrue(addIndex.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("ALGORITHM"), new SQLIdentifierExpr("IVF"))));
        Assert.assertTrue(addIndex.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("KEY_BLOCK_SIZE"), new SQLIntegerExpr(32))));
        Assert.assertTrue(addIndex.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("DISTANCEMEASURE"), new SQLIdentifierExpr("DotProduct"))));
    }

    public void test_1() {
        String sql = "create index ann idx on tb (a) KEY_BLOCK_SIZE=32 using hash comment 'hehe' DistanceMeasure = DotProduct ALGORITHM = IVF LOCK DEAFULT";
        SQLStatementParser parser = new MySqlStatementParser(sql,
                SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);

        Assert.assertTrue(result instanceof SQLCreateIndexStatement);
        SQLCreateIndexStatement statement = (SQLCreateIndexStatement) result;

        Assert.assertEquals(4, statement.getOptions().size());
        Assert.assertTrue(statement.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("ALGORITHM"), new SQLIdentifierExpr("IVF"))));
        Assert.assertTrue(statement.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("LOCK"), new SQLIdentifierExpr("DEAFULT"))));
        Assert.assertTrue(statement.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("KEY_BLOCK_SIZE"), new SQLIntegerExpr(32))));
        Assert.assertTrue(statement.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("DISTANCEMEASURE"), new SQLIdentifierExpr("DotProduct"))));
    }

    public void test_2() {
        String sql = "create table tb (" +
                "  x int," +
                "  ANN INDEX feature_idx0 (face_feature) KEY_BLOCK_SIZE=32 using hash comment 'hehe' DistanceMeasure = DotProduct ALGORITHM = IVF" +
                ")";
        SQLStatementParser parser = new MySqlStatementParser(sql,
                SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);

        Assert.assertTrue(result instanceof SQLCreateTableStatement);
        SQLCreateTableStatement statement = (SQLCreateTableStatement) result;
        Assert.assertEquals(2, statement.getTableElementList().size());
        Assert.assertTrue(statement.getTableElementList().get(1) instanceof MySqlTableIndex);
        MySqlTableIndex index = (MySqlTableIndex) statement.getTableElementList().get(1);

        Assert.assertEquals("DotProduct", index.getDistanceMeasure());
        Assert.assertEquals("IVF", index.getAlgorithm());
        Assert.assertEquals(new SQLIntegerExpr(32), index.getOption("KEY_BLOCK_SIZE"));

        Assert.assertEquals(index.getOptions().size(), 3);
        Assert.assertTrue(index.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("ALGORITHM"), new SQLIdentifierExpr("IVF"))));
        Assert.assertTrue(index.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("KEY_BLOCK_SIZE"), new SQLIntegerExpr(32))));
        Assert.assertTrue(index.getOptions().contains(new SQLAssignItem(new SQLIdentifierExpr("DISTANCEMEASURE"), new SQLIdentifierExpr("DotProduct"))));
    }

}

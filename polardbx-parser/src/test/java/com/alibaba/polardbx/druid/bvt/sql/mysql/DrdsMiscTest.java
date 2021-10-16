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

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;

import java.util.List;

public class DrdsMiscTest extends MysqlTest {

    public void testUpgradeRuleVersion() {
        String sql = "upgrade rule version to 233";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("UPGRADE RULE VERSION TO 233", SQLUtils.toMySqlString(result));
        Assert.assertEquals("upgrade rule version to 233", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testDowngradeRuleVersion() {
        String sql = "downgrade rule version to 233";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("DOWNGRADE RULE VERSION TO 233", SQLUtils.toMySqlString(result));
        Assert.assertEquals("downgrade rule version to 233", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testInspectRuleVersion() {
        String sql = "inspect rule version";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("INSPECT RULE VERSION", SQLUtils.toMySqlString(result));
        Assert.assertEquals("inspect rule version", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testInspectRuleVersionIgnoreManager() {
        String sql = "inspect rule version  ignore Manager";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("INSPECT RULE VERSION IGNORE MANAGER", SQLUtils.toMySqlString(result));
        Assert.assertEquals("inspect rule version ignore manager", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testRefreshLocalRules() {
        String sql = "refresh local rules";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("REFRESH LOCAL RULES", SQLUtils.toMySqlString(result));
        Assert.assertEquals("refresh local rules", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testClearSeqCache() {
        String sql = "clear sequence cache for test_seq";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("CLEAR SEQUENCE CACHE FOR test_seq", SQLUtils.toMySqlString(result));
        Assert.assertEquals("clear sequence cache for test_seq", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

    }

    public void testClearSeqCacheForAll() {
        String sql = "clear sequence cache for all";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("CLEAR SEQUENCE CACHE FOR ALL", SQLUtils.toMySqlString(result));
        Assert.assertEquals("clear sequence cache for all", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testInspectGroupSeqRange() {
        String sql = "inspect group sequence range for test_seq";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("INSPECT GROUP SEQUENCE RANGE FOR test_seq", SQLUtils.toMySqlString(result));
        Assert.assertEquals("inspect group sequence range for test_seq", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testPurgeTrans() {
        String sql = "purge  trans";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("PURGE TRANS", SQLUtils.toMySqlString(result));
        Assert.assertEquals("purge trans", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testShowTrans() {
        String sql = "show  trans";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("SHOW TRANS", SQLUtils.toMySqlString(result));
        Assert.assertEquals("show trans", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testMoveDatabase() {
        String sql = "move database db1,db2 to storageInstanceId1";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("MOVE DATABASE (DB1, DB2) TO STORAGEINSTANCEID1", SQLUtils.toMySqlString(result));
        Assert.assertEquals("move database (db1, db2) to storageinstanceid1", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        sql = "move database (db1,db2) to storageInstanceId1";
        parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        stmtList = parser.parseStatementList();

        result = stmtList.get(0);
        Assert.assertEquals("MOVE DATABASE (DB1, DB2) TO STORAGEINSTANCEID1", SQLUtils.toMySqlString(result));
        Assert.assertEquals("move database (db1, db2) to storageinstanceid1", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        sql = "move database (db1,db2) to storageInstanceId1,(db3,db4) to storageInstanceId2,(db5,db6) to storageInstanceId3";
        parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        stmtList = parser.parseStatementList();

        result = stmtList.get(0);
        Assert.assertEquals("MOVE DATABASE (DB5, DB6) TO STORAGEINSTANCEID3, (DB1, DB2) TO STORAGEINSTANCEID1, (DB3, DB4) TO STORAGEINSTANCEID2", SQLUtils.toMySqlString(result));
        Assert.assertEquals("move database (db5, db6) to storageinstanceid3, (db1, db2) to storageinstanceid1, (db3, db4) to storageinstanceid2", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));

        sql = "move database (db1,db2) to storageInstanceId1,(db3,db4) to storageInstanceId2,(db5,db6) to storageInstanceId1";
        parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        stmtList = parser.parseStatementList();

        result = stmtList.get(0);
        Assert.assertEquals("MOVE DATABASE (DB1, DB2, DB5, DB6) TO STORAGEINSTANCEID1, (DB3, DB4) TO STORAGEINSTANCEID2", SQLUtils.toMySqlString(result));
        Assert.assertEquals("move database (db1, db2, db5, db6) to storageinstanceid1, (db3, db4) to storageinstanceid2", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testMoveDatabaseWithHint() {
        String sql = "move database /*TDDL:SWITCH_GROUP_ONLY=TRUE*/ db1,db2 to storageInstanceId1";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc, SQLParserFeature.TDDLHint);
        List<SQLStatement> stmtList = parser.parseStatementList();

        Assert.assertEquals(stmtList.size(),1);
        Assert.assertEquals(stmtList.get(0).getHeadHintsDirect().size(),1);
        Assert.assertEquals(stmtList.get(0).getHeadHintsDirect().get(0).getText().toUpperCase(),"TDDL:SWITCH_GROUP_ONLY=TRUE");
        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("MOVE DATABASE (DB1, DB2) TO STORAGEINSTANCEID1", SQLUtils.toMySqlString(result));
        Assert.assertEquals("move database (db1, db2) to storageinstanceid1", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testShowMoveDatabase() {
        String sql = "show move database";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("SHOW MOVE DATABASE", SQLUtils.toMySqlString(result));
        Assert.assertEquals("show move database", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testShowMoveDatabaseWithWhere() {
        String sql = "show move database where a is not null order by b limit 1";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("SHOW MOVE DATABASE WHERE a IS NOT NULL ORDER BY b LIMIT 1", SQLUtils.toMySqlString(result));
        Assert.assertEquals("show move database where a is not null order by b limit 1", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }

    public void testCleanMoveDatabase() {
        String sql = "move database clean db1,db2";
        SQLStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.DrdsMisc);
        List<SQLStatement> stmtList = parser.parseStatementList();

        SQLStatement result = stmtList.get(0);
        Assert.assertEquals("MOVE DATABASE CLEAN DB1, DB2", SQLUtils.toMySqlString(result));
        Assert.assertEquals("move database clean db1, db2", SQLUtils.toMySqlString(result, SQLUtils.DEFAULT_LCASE_FORMAT_OPTION));
    }
}

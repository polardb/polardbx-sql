package com.alibaba.polardbx.optimizer.parse;

import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.sql.Connection;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class TableMetaParserTest {

    @Test
    public void testParseWithEncryptionTable() {
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
             MockedStatic<DbInfoManager> mockedDbInfoManager = Mockito.mockStatic(DbInfoManager.class);) {
            DbInfoRecord dbInfoRecord = new DbInfoRecord();
            dbInfoRecord.extra = JSONObject.parseObject("{\"encryption\":false}");
            Mockito.when(dbInfoManager.getDbInfo(any())).thenReturn(dbInfoRecord);

            mockedDbInfoManager.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);

            String tableDDL = "create table t1(a int, b int) ENCRYPTION='Y'";
            final MySqlCreateTableStatement stat = (MySqlCreateTableStatement) FastsqlUtils.parseSql(tableDDL).get(0);
            final TableMeta tm = new TableMetaParser().parse(stat);
            Assert.assertTrue(tm.isEncryption());
        }
    }

    @Test
    public void testParseWithEncryptionDB() {
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
             MockedStatic<DbInfoManager> mockedDbInfoManager = Mockito.mockStatic(DbInfoManager.class);) {
            DbInfoRecord dbInfoRecord = new DbInfoRecord();
            dbInfoRecord.extra = JSONObject.parseObject("{\"encryption\":true}");
            Mockito.when(dbInfoManager.getDbInfo(any())).thenReturn(dbInfoRecord);
            mockedDbInfoManager.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);

            String tableDDL = "create table t1(a int, b int) ENCRYPTION='N'";
            MySqlCreateTableStatement stat = (MySqlCreateTableStatement) FastsqlUtils.parseSql(tableDDL).get(0);
            TableMeta tm = new TableMetaParser().parse(stat);
            Assert.assertFalse("t1 is not encrypted", tm.isEncryption());

            tableDDL = "create table t1(a int, b int) ENCRYPTION='Y'";
            stat = (MySqlCreateTableStatement) FastsqlUtils.parseSql(tableDDL).get(0);
            tm = new TableMetaParser().parse(stat);
            Assert.assertTrue("t1 is encrypted", tm.isEncryption());

            tableDDL = "create table t1(a int, b int)";
            stat = (MySqlCreateTableStatement) FastsqlUtils.parseSql(tableDDL).get(0);
            tm = new TableMetaParser().parse(stat);
            Assert.assertFalse("t1 is not encrypted", tm.isEncryption());

            boolean result = TableMetaParser.parseEncryption("testdb", "");
            Assert.assertFalse(result);

            result = TableMetaParser.parseEncryption("testdb", "ENCRYPTION=\"Y\"");
            Assert.assertTrue(result);

            result = TableMetaParser.parseEncryption("testdb", "ENCRYPTION=\"N\"");
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testParseWithEncryption80DB() {
        DbInfoManager dbInfoManager = mock(DbInfoManager.class);
        try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
             MockedStatic<DbInfoManager> mockedDbInfoManager = Mockito.mockStatic(DbInfoManager.class);) {
            DbInfoRecord dbInfoRecord = new DbInfoRecord();
            dbInfoRecord.extra = JSONObject.parseObject("{\"encryption\":true}");
            Mockito.when(dbInfoManager.getDbInfo(any())).thenReturn(dbInfoRecord);
            mockedDbInfoManager.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);

            boolean result = TableMetaParser.parseEncryption("testdb", "");
            Assert.assertFalse(result);

            result = TableMetaParser.parseEncryption("testdb", "ENCRYPTION='Y'");
            Assert.assertTrue(result);

            result = TableMetaParser.parseEncryption("testdb", "ENCRYPTION='N'");
            Assert.assertFalse(result);
        }
    }

}

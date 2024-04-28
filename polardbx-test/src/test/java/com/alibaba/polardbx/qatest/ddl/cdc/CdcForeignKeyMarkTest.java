package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-12 09:50
 **/
public class CdcForeignKeyMarkTest extends CdcBaseTest {

    @Test
    public void testForeignKeys() throws SQLException {
        String dbName = "cdc_fk_test";
        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "create database " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "use " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        try (Statement stmt = tddlConnection.createStatement()) {
            executeAndCheck(dbName, stmt);
        }
    }

    private void executeAndCheck(String schemaName, Statement stmt) throws SQLException {
        // initialize
        String tableName = "device";
        DdlCheckContext checkContext = newDdlCheckContext();

        //create user1
        String createUser1 = "CREATE TABLE user1 (\n"
            + "\ta int PRIMARY KEY,\n"
            + "\tb int NOT NULL,\n"
            + "\tc int NOT NULL,\n"
            + "\td int NOT NULL,\n"
            + "\tINDEX b(b, c, d),\n"
            + "\tINDEX c(c, d)\n"
            + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`\n"
            + "DBPARTITION BY hash(a)";
        executeSql(stmt, createUser1);

        //create user2
        String createUser2 = "CREATE TABLE user2 (\n"
            + "\ta int PRIMARY KEY,\n"
            + "\tb int NOT NULL,\n"
            + "\tc int NOT NULL,\n"
            + "\td int NOT NULL,\n"
            + "\tINDEX b(b, c, d),\n"
            + "\tINDEX c(c, d)\n"
            + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`";
        executeSql(stmt, createUser2);

        //create device
        checkContext.updateAndGetMarkList(schemaName + "." + tableName);
        checkContext.updateAndGetMarkList(schemaName);
        String createPrimaryTable = "CREATE TABLE " + tableName + "(\n "
            + "\ta int PRIMARY KEY AUTO_INCREMENT,\n"
            + "\tb int NOT NULL,\n"
            + "\tc int NOT NULL,\n"
            + "\td int NOT NULL\n"
            + ") DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`\n"
            + "DBPARTITION BY hash(`a`)";
        executeSql(stmt, createPrimaryTable);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, createPrimaryTable);

        // add foreign key and check
        String addForeignKey1 = String.format("alter table `%s`.`%s` add constraint `device_ibfk_1` foreign key (`b`)"
            + " REFERENCES `user2` (`a`)", schemaName, tableName);
        executeSql(stmt, addForeignKey1);

        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, addForeignKey1);
        DdlRecordInfo ddlRecordInfo = checkContext.updateAndGetMarkList(schemaName + "." + tableName).get(0);
        assertSqlEquals(addForeignKey1, ddlRecordInfo.getDdlExtInfo().getOriginalDdl());
        Assert.assertEquals(Boolean.TRUE, ddlRecordInfo.getDdlExtInfo().getForeignKeysDdl());

        // add foreign key and check
        String addForeignKey2 =
            String.format("alter table `%s`.`%s` add constraint `my_ibfk_1` foreign key `fk1` (`c`)"
                + " REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE", schemaName, tableName);
        executeSql(stmt, addForeignKey2);

        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, addForeignKey2);
        ddlRecordInfo = checkContext.updateAndGetMarkList(schemaName + "." + tableName).get(0);
        assertSqlEquals(addForeignKey2, ddlRecordInfo.getDdlExtInfo().getOriginalDdl());
        Assert.assertEquals(Boolean.TRUE, ddlRecordInfo.getDdlExtInfo().getForeignKeysDdl());

        // drop foreign key and check
        // 匿名foreign key会添加固定后缀与序号_ibfk_{num} see @link com.alibaba.polardbx.optimizer.utils.ForeignKeyUtils#getForeignKeyConstraintName
        String dropForeignKey1 = String.format(
            "alter table `%s`.`%s` drop foreign key `device_ibfk_1`", schemaName, tableName);
        executeSql(stmt, dropForeignKey1);

        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, dropForeignKey1);
        ddlRecordInfo = checkContext.updateAndGetMarkList(schemaName + "." + tableName).get(0);
        assertSqlEquals(dropForeignKey1, ddlRecordInfo.getDdlExtInfo().getOriginalDdl());
        Assert.assertEquals(Boolean.TRUE, ddlRecordInfo.getDdlExtInfo().getForeignKeysDdl());
        Assert.assertNull(ddlRecordInfo.getDdlExtInfo().getFlags2());

        // drop foreign key and check
        String dropForeignKey2 =
            String.format("alter table `%s`.`%s` drop foreign key `my_ibfk_1`", schemaName, tableName);
        executeSql(stmt, "set foreign_key_checks = 0");
        executeSql(stmt, dropForeignKey2);

        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, dropForeignKey2);
        ddlRecordInfo = checkContext.updateAndGetMarkList(schemaName + "." + tableName).get(0);
        assertSqlEquals(dropForeignKey2, ddlRecordInfo.getDdlExtInfo().getOriginalDdl());
        Assert.assertEquals("OPTION_NO_FOREIGN_KEY_CHECKS,", ddlRecordInfo.getDdlExtInfo().getFlags2());
        Assert.assertEquals(Boolean.TRUE, ddlRecordInfo.getDdlExtInfo().getForeignKeysDdl());

        // drop table
        String dropTable = "drop table `" + tableName + "`";
        executeSql(stmt, dropTable);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, dropTable);

        // re-create table
        String createWithForeignKeys = "create table " + tableName + "\n"
            + "(   a int auto_increment primary key,\n"
            + "    b int not null,\n"
            + "    c int not null,\n"
            + "    d int not null,\n"
            + "    key (`c`),\n"
            + "    foreign key (`b`) REFERENCES `user2` (`a`),\n"
            + "    foreign key `fk` (`b`) REFERENCES `user2` (`b`),\n"
            + "    constraint `my_ibfk` foreign key (`b`) REFERENCES `user2` (`c`),\n"
            + "    constraint `my_ibfk_1` foreign key `fk1` (`c`) REFERENCES `user2` (`c`),\n"
            + "    foreign key (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
            + "    foreign key (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
            + "    foreign key (`c`) REFERENCES `user1` (`c`),\n"
            + "    foreign key (`d`) REFERENCES `device` (`c`),\n"
            + "    constraint `fk_device_user` foreign key (`b` , `c` , `d`)\n"
            + "       REFERENCES `user2` (`b` , `c` , `d`)\n"
            + ")dbpartition by hash(`b`)";
        executeSql(stmt, "set foreign_key_checks = 1");
        executeSql(stmt, createWithForeignKeys);

        commonCheckExistsAfterDdl(checkContext, schemaName, tableName, "create table " + tableName + "\n"
            + "(   a int auto_increment primary key,\n"
            + "    b int not null,\n"
            + "    c int not null,\n"
            + "    d int not null,\n"
            + "    key (`c`),\n"
            + "    CONSTRAINT `device_ibfk_1` foreign key (`b`) REFERENCES `user2` (`a`),\n"
            + "    CONSTRAINT `device_ibfk_2` foreign key `fk` (`b`) REFERENCES `user2` (`b`),\n"
            + "    constraint `my_ibfk` foreign key (`b`) REFERENCES `user2` (`c`),\n"
            + "    constraint `my_ibfk_1` foreign key `fk1` (`c`) REFERENCES `user2` (`c`),\n"
            + "    CONSTRAINT `device_ibfk_3` foreign key (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
            + "    CONSTRAINT `device_ibfk_4` foreign key (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
            + "    CONSTRAINT `device_ibfk_5` foreign key (`c`) REFERENCES `user1` (`c`),\n"
            + "    CONSTRAINT `device_ibfk_6` foreign key (`d`) REFERENCES `device` (`c`),\n"
            + "    constraint `fk_device_user` foreign key (`b` , `c` , `d`)\n"
            + "       REFERENCES `user2` (`b` , `c` , `d`)\n"
            + ")dbpartition by hash(`b`)");
        ddlRecordInfo = checkContext.updateAndGetMarkList(schemaName + "." + tableName).get(0);
        Assert.assertEquals(Boolean.TRUE, ddlRecordInfo.getDdlExtInfo().getForeignKeysDdl());
        Assert.assertNull(ddlRecordInfo.getDdlExtInfo().getFlags2());
    }
}

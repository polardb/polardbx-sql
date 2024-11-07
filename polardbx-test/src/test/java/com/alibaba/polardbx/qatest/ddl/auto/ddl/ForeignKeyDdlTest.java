package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@ReplicaIgnore(
    ignoreReason = "目前创建外键的时候会同步创建一个同名的索引，导致了Duplicate Key，导致下游复制中断，暂时忽略")
public class ForeignKeyDdlTest extends PartitionTestBase {
    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

    private static final String FOREIGN_KEY_CHECKS = "FOREIGN_KEY_CHECKS=TRUE";
    private static final String FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE = "FOREIGN_KEY_CHECKS_FOR_UPDATE_DELETE=TRUE";

    private static final String dataBaseName = "ForeignKeyDdlDB_new";

    private static final String[] PART_DEFS = new String[] {
        "PARTITION BY KEY(`a`)" + "\nPARTITIONS 7",
        "PARTITION BY KEY(`b`)" + "\nPARTITIONS 7",
        "PARTITION BY KEY(`b`,`c`)" + "\nPARTITIONS 7",
        "SINGLE",
        "BROADCAST"
    };

    private static final String CREATE_USER1 = "create table user1\n"
        + "(   a int auto_increment primary key,\n"
        + "    b int not null,\n"
        + "    c int not null,\n"
        + "    d int not null,\n"
        + "    index (b,c,d),\n"
        + "    index (c,d)\n"
        + ") %s";

    private static final String CREATE_USER2 = "create table user2\n"
        + "(   a int auto_increment primary key,\n"
        + "    b int not null,\n"
        + "    c int not null,\n"
        + "    d int not null,\n"
        + "    index (b,c,d),\n"
        + "    index (c,d)\n"
        + ") %s";

    private static final String CREATE_DEVICE = "create table device\n"
        + "(   a int auto_increment primary key,\n"
        + "    b int not null,\n"
        + "    c int not null,\n"
        + "    d int not null,\n"
        + "    foreign key (`b`) REFERENCES `user1` (`b`)\n"
        + ") %s";

    private static final String CREATE_CHARSET_P = "create table charset_p\n"
        + "(   a int auto_increment primary key,\n"
        + "    b varchar(20) not null,\n"
        + "    c varchar(20) not null,\n"
        + "    d varchar(20) not null,\n"
        + "    index (b,c,d),\n"
        + "    index (c,d)\n"
        + ") %s";

    private static final String CREATE_CHARSET_C = "create table charset_c\n"
        + "(   a int auto_increment primary key,\n"
        + "    b varchar(20) not null,\n"
        + "    c varchar(20) not null,\n"
        + "    d varchar(20) not null,\n"
        + "    foreign key (`b`) REFERENCES `charset_p` (`b`)\n"
        + ") %s";

    private static final String CREATE_DEVICE_SAME_NAME = "create table device_same\n"
        + "(   a int primary key,\n"
        + "    b int not null,\n"
        + "    c int not null,\n"
        + "    d int not null,\n"
        + "    KEY `device_ibfk_1` (`b`),"
        + "    KEY `device_ibfk_2` (`b`),"
        + "    constraint `device_ibfk_1` foreign key (`b`) REFERENCES `user1` (`b`)\n"
        + ") %s";

    @Before
    public void before() {
        doReCreateDatabase();
    }

    @After
    public void after() {
        doClearDatabase();
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String createDbHint = "/*+TDDL({\"extra\":{\"SHARD_DB_COUNT_EACH_STORAGE_INST_FOR_STMT\":\"4\"}})*/";
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = createDbHint + "create database " + dataBaseName + " partition_mode = 'auto'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + dataBaseName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql =
            "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists " + dataBaseName;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    @Test
    public void testCreateTableWithFk() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists("device");
            dropTableIfExists("user1");
            dropTableIfExists("user2");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER1, partitionDef1));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER2, partitionDef1));

            for (String partitionDef2 : PART_DEFS) {
                if (partitionDef2.contains("`b`,`c`") || partitionDef2.contains("SINGLE") || partitionDef2.contains(
                    "BROADCAST")) {
                    // show create table has auto-shared-key
                    continue;
                }
                System.out.println(partitionDef1 + " | " + partitionDef2);

                dropTableIfExists("device");

                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    String.format("create table device\n"
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
                        + ")%s", partitionDef2));

                String createTableString = showCreateTable(tddlConnection, "device");

                assertEquals(String.format("CREATE TABLE `device` (\n"
                    + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "\t`b` int(11) NOT NULL,\n"
                    + "\t`c` int(11) NOT NULL,\n"
                    + "\t`d` int(11) NOT NULL,\n"
                    + "\tPRIMARY KEY (`a`),\n"
                    + "\tCONSTRAINT `device_ibfk_1` FOREIGN KEY (`b`) REFERENCES `user2` (`a`),\n"
                    + "\tCONSTRAINT `device_ibfk_2` FOREIGN KEY (`b`) REFERENCES `user2` (`b`),\n"
                    + "\tCONSTRAINT `my_ibfk` FOREIGN KEY (`b`) REFERENCES `user2` (`c`),\n"
                    + "\tCONSTRAINT `my_ibfk_1` FOREIGN KEY (`c`) REFERENCES `user2` (`c`),\n"
                    + "\tCONSTRAINT `device_ibfk_3` FOREIGN KEY (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
                    + "\tCONSTRAINT `device_ibfk_4` FOREIGN KEY (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
                    + "\tCONSTRAINT `device_ibfk_5` FOREIGN KEY (`c`) REFERENCES `user1` (`c`),\n"
                    + "\tCONSTRAINT `device_ibfk_6` FOREIGN KEY (`d`) REFERENCES `device` (`c`),\n"
                    + "\tCONSTRAINT `fk_device_user` FOREIGN KEY (`b`, `c`, `d`) REFERENCES `user2` (`b`, `c`, `d`),\n"
                    + "\tKEY `c` (`c`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);

                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key device_ibfk_1");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key device_ibfk_2");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key my_ibfk");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key my_ibfk_1");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key device_ibfk_3");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key device_ibfk_4");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key device_ibfk_5");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key device_ibfk_6");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key fk_device_user");

                createTableString = showCreateTable(tddlConnection, "device");

                assertEquals(String.format("CREATE TABLE `device` (\n"
                    + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "\t`b` int(11) NOT NULL,\n"
                    + "\t`c` int(11) NOT NULL,\n"
                    + "\t`d` int(11) NOT NULL,\n"
                    + "\tPRIMARY KEY (`a`),\n"
                    + "\tKEY `c` (`c`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);

                // should fail if referenced table does not have the index
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    String.format("create table device1\n"
                        + "(   a int auto_increment primary key,\n"
                        + "    b int not null,\n"
                        + "    c int not null,\n"
                        + "    d int not null,\n"
                        + "    key (`c`),\n"
                        + "    foreign key (`b`) REFERENCES `user2` (`d`)\n"
                        + ")%s", partitionDef2), "");

                // should fail if referenced table not exists
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    String.format("create table device1\n"
                        + "(   a int auto_increment primary key,\n"
                        + "    b int not null,\n"
                        + "    c int not null,\n"
                        + "    d int not null,\n"
                        + "    key (`c`),\n"
                        + "    foreign key (`b`) REFERENCES `user20` (`c`)\n"
                        + ")%s", partitionDef2), "");
            }
        }
    }

    @Test
    public void testAlterTableAddFk() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists("device");
            dropTableIfExists("user1");
            dropTableIfExists("user2");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER1, partitionDef1));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER2, partitionDef1));
            for (String partitionDef2 : PART_DEFS) {
                if (partitionDef2.contains("`b`") || partitionDef2.contains("SINGLE") || partitionDef2.contains(
                    "BROADCAST")) {
                    // show create table has auto-shared-key
                    continue;
                }

                System.out.println(partitionDef1 + " | " + partitionDef2);

                dropTableIfExists("device");
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    String.format("create table device\n"
                        + "(   a int auto_increment primary key,\n"
                        + "    b int not null,\n"
                        + "    c int not null,\n"
                        + "    d int not null\n"
                        + ")%s", partitionDef2));

                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "alter table `device` add foreign key (`b`) REFERENCES `user2` (`a`)");
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "alter table `device` add constraint `my_ibfk_1` foreign key `fk1` (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE");
                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    "alter table `device` add constraint `fk_device_user` foreign key (`b` , `c` , `d`) REFERENCES `user2` (`b` , `c` , `d`)");

                // should fail if referenced table does not have the index
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    "alter table `device` add foreign key (`d`) REFERENCES `user2` (`d`)", "");

                // should fail if referenced table not exists
                JdbcUtil.executeUpdateFailed(tddlConnection,
                    "alter table `device` add foreign key (`d`) REFERENCES `user20` (`c`)", "");

                String createTableString = showCreateTable(tddlConnection, "device");

                assertEquals(String.format("CREATE TABLE `device` (\n"
                    + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "\t`b` int(11) NOT NULL,\n"
                    + "\t`c` int(11) NOT NULL,\n"
                    + "\t`d` int(11) NOT NULL,\n"
                    + "\tPRIMARY KEY (`a`),\n"
                    + "\tCONSTRAINT `device_ibfk_1` FOREIGN KEY (`b`) REFERENCES `user2` (`a`),\n"
                    + "\tCONSTRAINT `my_ibfk_1` FOREIGN KEY (`c`) REFERENCES `user2` (`c`) ON DELETE CASCADE ON UPDATE CASCADE,\n"
                    + "\tCONSTRAINT `fk_device_user` FOREIGN KEY (`b`, `c`, `d`) REFERENCES `user2` (`b`, `c`, `d`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);

                // drop indexes on referenced tables should fail
                JdbcUtil.executeUpdateFailed(tddlConnection, "alter table `user2` drop index `b`", "");

                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key device_ibfk_1");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key my_ibfk_1");
                JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device drop foreign key fk_device_user");

                createTableString = showCreateTable(tddlConnection, "device");

                assertEquals(String.format("CREATE TABLE `device` (\n"
                    + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "\t`b` int(11) NOT NULL,\n"
                    + "\t`c` int(11) NOT NULL,\n"
                    + "\t`d` int(11) NOT NULL,\n"
                    + "\tPRIMARY KEY (`a`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);
            }
        }
    }

    @Test
    public void truncateOrDropTableWithFkReferred() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists("device");
            dropTableIfExists("user1");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER1, partitionDef1));

            for (String partitionDef2 : PART_DEFS) {
                System.out.println(partitionDef1 + " | " + partitionDef2);
                dropTableIfExists("device");
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_DEVICE, partitionDef2));

                JdbcUtil.executeUpdateFailed(tddlConnection, "DROP TABLE user1", "");
                JdbcUtil.executeUpdateFailed(tddlConnection, "TRUNCATE TABLE user1", "");
            }
        }
    }

    @Test
    public void renameTableWithFkReferred() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists("device");
            dropTableIfExists("user1");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER1, partitionDef1));

            for (String partitionDef2 : PART_DEFS) {
                if (partitionDef2.contains("`b`") || partitionDef2.contains("SINGLE") || partitionDef2.contains(
                    "BROADCAST")) {
                    // show create table has auto-shared-key
                    continue;
                }
                System.out.println(partitionDef1 + " | " + partitionDef2);
                dropTableIfExists("device");
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_DEVICE, partitionDef2));

                JdbcUtil.executeUpdateSuccess(tddlConnection, "rename table user1 to user100");

                JdbcUtil.executeUpdateFailed(tddlConnection, "drop table user100", "");

                String createTableString = showCreateTable(tddlConnection, "device");

                assertEquals(String.format("CREATE TABLE `device` (\n"
                    + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                    + "\t`b` int(11) NOT NULL,\n"
                    + "\t`c` int(11) NOT NULL,\n"
                    + "\t`d` int(11) NOT NULL,\n"
                    + "\tPRIMARY KEY (`a`),\n"
                    + "\tCONSTRAINT `device_ibfk_1` FOREIGN KEY (`b`) REFERENCES `user100` (`b`)\n"
                    + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);

                JdbcUtil.executeUpdateSuccess(tddlConnection, "rename table user100 to user1");
            }
        }
    }

    @Test
    public void changeColumnWithFkReferred() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        dropTableIfExists("device");
        dropTableIfExists("user1");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_USER1, "PARTITION BY KEY(`a`) PARTITIONS 7"));

        for (String partitionDef2 : PART_DEFS) {
            if (partitionDef2.contains("`b`,`c`")) {
                // show create table has auto-shared-key
                continue;
            }
            System.out.println(partitionDef2);
            dropTableIfExists("device");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_DEVICE, partitionDef2));

            // can not change column type
            JdbcUtil.executeUpdateFailed(tddlConnection, "alter table user1 change column b b100 bigint", "");

            JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table user1 change column b b100 int");

            String createTableString = showCreateTable(tddlConnection, "device");

            assertEquals(String.format("CREATE TABLE `device` (\n"
                + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                + "\t`b` int(11) NOT NULL,\n"
                + "\t`c` int(11) NOT NULL,\n"
                + "\t`d` int(11) NOT NULL,\n"
                + "\tPRIMARY KEY (`a`),\n"
                + "\tCONSTRAINT `device_ibfk_1` FOREIGN KEY (`b`) REFERENCES `user1` (`b100`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table user1 change column b100 b int");

            if (partitionDef2.contains("`b`")) {
                // not allow change sharding key
                continue;
            }

            // exchange b and c in user1
            JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table user1 change column b c int, change c b int");

            createTableString = showCreateTable(tddlConnection, "device");

            assertEquals(String.format("CREATE TABLE `device` (\n"
                + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                + "\t`b` int(11) NOT NULL,\n"
                + "\t`c` int(11) NOT NULL,\n"
                + "\t`d` int(11) NOT NULL,\n"
                + "\tPRIMARY KEY (`a`),\n"
                + "\tCONSTRAINT `device_ibfk_1` FOREIGN KEY (`b`) REFERENCES `user1` (`c`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);

            // exchange b and c in device
            JdbcUtil.executeUpdateSuccess(tddlConnection, "alter table device change column b c int, change c b int");

            createTableString = showCreateTable(tddlConnection, "device");

            assertEquals(String.format("CREATE TABLE `device` (\n"
                + "\t`a` int(11) NOT NULL AUTO_INCREMENT,\n"
                + "\t`c` int(11) DEFAULT NULL,\n"
                + "\t`b` int(11) DEFAULT NULL,\n"
                + "\t`d` int(11) NOT NULL,\n"
                + "\tPRIMARY KEY (`a`),\n"
                + "\tCONSTRAINT `device_ibfk_1` FOREIGN KEY (`c`) REFERENCES `user1` (`c`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n%s", partitionDef2), createTableString);
        }
    }

    @Test
    public void testFkOptionRestrict() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        dropTableIfExists("device");
        dropTableIfExists("user1");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_USER1, "PARTITION BY KEY(`a`) PARTITIONS 7"));

        for (String partitionDef2 : PART_DEFS) {

            System.out.println(partitionDef2);
            dropTableIfExists("device");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_DEVICE, partitionDef2));

            // do not support set default
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format(
                    "alter table `device` add foreign key (`d`) REFERENCES `user1` (`c`) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT ",
                    partitionDef2), "");

            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("create table device1\n"
                    + "(   a int auto_increment primary key,\n"
                    + "    b int not null,\n"
                    + "    c int not null,\n"
                    + "    d int not null,\n"
                    + "    key (`c`),\n"
                    + "    foreign key (`b`) REFERENCES `user1` (`c`) ON DELETE SET DEFAULT ON UPDATE SET DEFAULT\n"
                    + ")%s", partitionDef2), "");

            // fk columns can not be null or primary key when option is set null
            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format(
                    "alter table `device` add foreign key (`d`) REFERENCES `user1` (`c`) ON DELETE SET NULL ON UPDATE SET NULL ",
                    partitionDef2), "");

            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("create table device1\n"
                    + "(   a int auto_increment primary key,\n"
                    + "    b int not null,\n"
                    + "    c int not null,\n"
                    + "    d int not null,\n"
                    + "    key (`c`),\n"
                    + "    foreign key (`b`) REFERENCES `user1` (`c`) ON DELETE SET NULL ON UPDATE SET NULL\n"
                    + ")%s", partitionDef2), "");
        }
    }

    protected Long getTableGroupId(String tableName) {
        String query = String.format(
            "select group_id from table_partitions where table_schema='%s' and table_name='%s' and part_level='0'",
            dataBaseName, tableName);
        return queryTableGroup(query);
    }

    @Test
    public void repartitionFkTest() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String schemaName = dataBaseName;

        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists("device");
            dropTableIfExists("user1");

            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER1, partitionDef1));
            for (String partitionDef2 : PART_DEFS) {
                if (partitionDef1.substring(0, 3).equals(partitionDef2.substring(0, 3))) {
                    continue;
                }
                dropTableIfExists("device");
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_DEVICE, partitionDef1));
                for (String partitionDef3 : PART_DEFS) {
                    if (partitionDef3.substring(0, 3).equals(partitionDef2.substring(0, 3))) {
                        continue;
                    }
                    System.out.println(partitionDef1 + "|" + partitionDef2 + "|" + partitionDef3);

                    JdbcUtil.executeUpdateSuccess(tddlConnection,
                        String.format("alter table device %s", partitionDef3));

                    long pushDown = 0L;
                    String sql = String.format(
                        "select PUSH_DOWN from foreign_key where SCHEMA_NAME = '%s' and TABLE_NAME = '%s'", schemaName,
                        "device");
                    try (Connection metaDbConn = getMetaConnection();
                        Statement stmt = metaDbConn.createStatement();
                        ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, "device"))) {
                        while (rs.next()) {
                            pushDown = rs.getLong(1);
                        }
                    }

                    if (partitionDef3.equals(partitionDef1) && (partitionDef3.equals("SINGLE") || partitionDef3.equals(
                        "BROADCAST"))) {

                        if (getTableGroupId("user1").equals(getTableGroupId("device"))) {
                            assertEquals(1L, pushDown);
                        }
                    } else {
                        assertTrue(pushDown == 2 || pushDown == 3);
                    }
                }
            }
        }
    }

    @Test
    public void repartitionFkSameTableTest() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        String schemaName = dataBaseName;

        for (String partitionDef1 : PART_DEFS) {
            dropTableIfExists("device");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("create table device\n"
                    + "(   a int primary key,\n"
                    + "    b int not null,\n"
                    + "    c int not null,\n"
                    + "    d int not null,\n"
                    + "    key (`c`),\n"
                    + "    foreign key (`d`) REFERENCES `device` (`c`)\n"
                    + ")%s", partitionDef1));

            for (String partitionDef3 : PART_DEFS) {
                if (partitionDef3.equals(partitionDef1)) {
                    continue;
                }
                System.out.println(partitionDef1 + "|" + partitionDef3);

                JdbcUtil.executeUpdateSuccess(tddlConnection,
                    String.format("alter table device %s", partitionDef3));

                long pushDown = 0L;
                String sql = String.format(
                    "select PUSH_DOWN from foreign_key where SCHEMA_NAME = '%s' and TABLE_NAME = '%s'", schemaName,
                    "device");
                try (Connection metaDbConn = getMetaConnection();
                    Statement stmt = metaDbConn.createStatement();
                    ResultSet rs = stmt.executeQuery(String.format(sql, schemaName, "device"))) {
                    while (rs.next()) {
                        pushDown = rs.getLong(1);
                    }
                }

                if ((partitionDef3.equalsIgnoreCase("SINGLE")
                    || partitionDef3.equalsIgnoreCase("BROADCAST"))) {
                    assertEquals(1L, pushDown);
                } else {
                    assertTrue(pushDown == 2 || pushDown == 3);
                }
            }
        }
    }

    @Test
    public void CreateTableDifferentCharsetWithFkReferred() throws SQLException {
        if (isMySQL80()) {
            return;
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        dropTableIfExists("charset_c");
        dropTableIfExists("charset_p");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_CHARSET_P, "DEFAULT CHARSET = utf8mb4 PARTITION BY KEY(`a`) PARTITIONS 7"));
        JdbcUtil.executeUpdateFailed(tddlConnection,
            String.format(CREATE_CHARSET_C, "DEFAULT CHARSET = utf8 PARTITION BY KEY(`a`) PARTITIONS 7"),
            "Cannot add foreign key constraint due to different charset or collation");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_CHARSET_C, "PARTITION BY KEY(`a`) PARTITIONS 7"));

        dropTableIfExists("charset_c");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_CHARSET_C, "DEFAULT CHARSET = UTF8MB4 PARTITION BY KEY(`a`) PARTITIONS 7"));

        dropTableIfExists("charset_c");
        dropTableIfExists("charset_p");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_CHARSET_P,
            "DEFAULT CHARSET = utf8mb4  DEFAULT COLLATE = utf8mb4_general_ci PARTITION BY KEY(`a`) PARTITIONS 7"));
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(CREATE_CHARSET_C,
                "DEFAULT CHARSET = utf8mb4  DEFAULT COLLATE = utf8mb4_unicode_ci PARTITION BY KEY(`a`) PARTITIONS 7"),
            "Cannot add foreign key constraint due to different charset or collation");

        dropTableIfExists("charset_p");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_CHARSET_P,
            "DEFAULT CHARSET = utf8mb4  DEFAULT COLLATE = utf8mb4_general_ci PARTITION BY KEY(`a`) PARTITIONS 7"));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_CHARSET_C, "PARTITION BY KEY(`a`) PARTITIONS 7"));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table charset_c add column e varchar(20) not null charset utf8");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table charset_c add constraint foreign key (`e`) REFERENCES `charset_p` (`b`)",
            "Cannot add foreign key constraint due to different charset or collation");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "alter table charset_c add column f varchar(20) not null charset utf8mb4 collate utf8mb4_unicode_ci");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "alter table charset_c add constraint foreign key (`f`) REFERENCES `charset_p` (`b`)",
            "Cannot add foreign key constraint due to different charset or collation");

        dropTableIfExists("charset_c");
        dropTableIfExists("charset_p");
    }

    @Test
    public void FkRefIndexNameUpdateTest() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        dropTableIfExists("device");
        dropTableIfExists("user1");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set foreign_key_checks = 0");

        // create child table first
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_DEVICE, "PARTITION BY KEY(`a`) PARTITIONS 7"));

        ResultSet rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
                "SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '%s' AND TABLE_NAME = 'device'",
                dataBaseName));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getString(6), "");

        // create parent table
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_USER1, "PARTITION BY KEY(`a`) PARTITIONS 7"));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '%s' AND TABLE_NAME = 'device'",
            dataBaseName));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getString(6), "b");

        // change fk index
        JdbcUtil.executeUpdateSuccess(tddlConnection, "ALTER TABLE user1 ADD KEY(`b`, `c`, `d`)");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "ALTER TABLE user1 DROP KEY `b`");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '%s' AND TABLE_NAME = 'device'",
            dataBaseName));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getString(6), "b_2");

        // rename fk index
        JdbcUtil.executeUpdateSuccess(tddlConnection, "ALTER TABLE user1 RENAME INDEX `b_2` to `b_3`");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '%s' AND TABLE_NAME = 'device'",
            dataBaseName));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getString(6), "b_3");

        // drop parent table
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE user1");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT * FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = '%s' AND TABLE_NAME = 'device'",
            dataBaseName));
        Assert.assertTrue(rs.next());
        assertEquals(rs.getString(6), "");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set foreign_key_checks = 1");
    }

    @Test
    public void FkIndexNameSensitiveTest() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        dropTableIfExists("device");
        dropTableIfExists("user");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table user\n"
                + "(   a int auto_increment primary key,\n"
                + "    B int not null,\n"
                + "    c int not null,\n"
                + "    d int not null,\n"
                + "    key (`B`)\n"
                + ")");

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table device\n"
                + "(   a int auto_increment primary key,\n"
                + "    b int not null,\n"
                + "    c int not null,\n"
                + "    d int not null,\n"
                + "    key (`c`),\n"
                + "    foreign key (`b`) REFERENCES `user` (`b`) ON DELETE CASCADE ON UPDATE CASCADE\n"
                + ")");

        dropTableIfExists("device");
        dropTableIfExists("user");
    }

    @Test
    public void testCreateFkWithSameNameIndex() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET ENABLE_FOREIGN_KEY = true");

        dropTableIfExists("device");
        dropTableIfExists("user1");

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_USER1, "partition by hash(a)"));

        for (String partitionDef2 : PART_DEFS) {

            System.out.println(partitionDef2);
            dropTableIfExists("device_same");
            // create table with fk
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_DEVICE_SAME_NAME, partitionDef2));
            // create table and create fk
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format(
                    "alter table `device_same` add constraint `device_ibfk_2` foreign key (`b`) REFERENCES `user1` (`c`)",
                    partitionDef2));
        }
    }
}

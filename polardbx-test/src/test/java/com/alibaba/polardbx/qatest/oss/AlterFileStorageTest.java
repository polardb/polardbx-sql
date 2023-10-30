package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.gms.engine.CachePolicy;
import com.alibaba.polardbx.gms.engine.DeletePolicy;
import com.alibaba.polardbx.gms.engine.FileStorageInfoAccessor;
import com.alibaba.polardbx.gms.engine.FileStorageInfoRecord;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Ignore("to add oss test")
public class AlterFileStorageTest extends BaseTestCase {

    private static String testDataBase = "AlterFileStorageTestDatabase";

    private static String testDataBase2 = "AlterFileStorageTestDatabase2";

    private static Engine engine = PropertiesUtil.engine();

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @Before
    public void initTestDatabase() {
        try (Connection conn = getPolardbxConnection()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase));
            statement.execute(String.format("use %s", testDataBase));

            statement.execute(String.format("drop database if exists %s ", testDataBase2));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase2));
            statement.execute(String.format("use %s", testDataBase2));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        initLocalDisk();
    }

    private void initLocalDisk() {
        try (Connection connection = getMetaConnection()) {
            try {
                FileStorageInfoAccessor fileStorageInfoAccessor = new FileStorageInfoAccessor();
                fileStorageInfoAccessor.setConnection(connection);

                FileStorageInfoRecord record1 = new FileStorageInfoRecord();
                record1.instId = "";
                record1.engine = "local_disk";
                record1.fileUri = "file:///tmp/local-orc-dir/";
                record1.fileSystemConf = "";
                record1.priority = 1;
                record1.regionId = "";
                record1.availableZoneId = "";
                record1.cachePolicy = CachePolicy.META_AND_DATA_CACHE.getValue();
                record1.deletePolicy = DeletePolicy.MASTER_ONLY.getValue();
                record1.status = 1;

                List<FileStorageInfoRecord> fileStorageInfoRecordList = new ArrayList<>();
                fileStorageInfoRecordList.add(record1);
                fileStorageInfoAccessor.insertIgnore(fileStorageInfoRecordList);
                Statement statement = connection.createStatement();
                statement.execute(
                    "update config_listener set op_version = op_version + 1 where data_id = 'polardbx.file.storage.info'");
                Thread.sleep(5000);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * ALTER FILESTORAGE OSS purge before TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStoragePurgeBeforeTimeStamp() {
        try (Connection connection = getConnection()) {
            String ossTableName = "testAlterFileStoragePurgeBeforeTimeStamp";
            FileStorageTestUtil.createFileStorageTableWith10000Rows(ossTableName, engine, connection);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show files from " + ossTableName);
            while (resultSet.next()) {
                String fileName = resultSet.getString("FILE_NAME");

                LocalDateTime beforeDropTime = LocalDateTime.now();
                Thread.sleep(2000);

                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    statement2.execute(String.format("alter table %s drop file '%s'", ossTableName, fileName));

                    Thread.sleep(2000);
                    LocalDateTime afterDropTime = LocalDateTime.now();

                    long countBeforeDropTime =
                        FileStorageTestUtil.countAsOfTimeStamp(connection2, ossTableName, beforeDropTime);
                    long countAfterDropTable =
                        FileStorageTestUtil.countAsOfTimeStamp(connection2, ossTableName, afterDropTime);

                    Assert.assertTrue(countBeforeDropTime > countAfterDropTable);

                    statement2.executeUpdate(String.format(
                        "/*+TDDL:cmd_extra(BACKUP_OSS_PERIOD=0)*/ ALTER FILESTORAGE '%s' PURGE BEFORE TIMESTAMP '%s'",
                        engine.name(),
                        String.format("%04d-%02d-%02d %02d:%02d:%02d",
                            afterDropTime.getYear(),
                            afterDropTime.getMonthValue(),
                            afterDropTime.getDayOfMonth(),
                            afterDropTime.getHour(),
                            afterDropTime.getMinute(),
                            afterDropTime.getSecond())));

                    countBeforeDropTime =
                        FileStorageTestUtil.countAsOfTimeStamp(connection2, ossTableName, beforeDropTime);
                    countAfterDropTable =
                        FileStorageTestUtil.countAsOfTimeStamp(connection2, ossTableName, afterDropTime);

                    Assert.assertTrue(countBeforeDropTime == countAfterDropTable);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER FILESTORAGE OSS AS OF TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStorageAsOfTimeStamp() {
        try (Connection connection = getConnection()) {
            String tableName = "testAlterFileStorageAsOfTimeStampTable";
            FileStorageTestUtil.createFileStorageTableWith10000Rows(tableName, engine, connection);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("show files from " + tableName);
            int lastCount = Integer.MAX_VALUE;
            List<LocalDateTime> dateTimeList = new ArrayList<>();
            dateTimeList.add(LocalDateTime.now());
            while (resultSet.next()) {
                String fileName = resultSet.getString("FILE_NAME");
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    statement2.execute(String.format("alter table %s drop file '%s'", tableName, fileName));
                    Thread.sleep(1000);
                    dateTimeList.add(LocalDateTime.now());
                    Thread.sleep(1000);
                    ResultSet resultSet2 = statement2.executeQuery(String.format("select count(*) from %s", tableName));
                    resultSet2.next();
                    int count = resultSet2.getInt(1);
                    Assert.assertTrue(lastCount > count);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            statement.execute(String.format("alter filestorage '%s' backup", engine.name()));

            lastCount = Integer.MIN_VALUE;
            for (int i = dateTimeList.size() - 1; i >= 0; i--) {
                try (Connection connection2 = getConnection()) {
                    Statement statement2 = connection2.createStatement();
                    LocalDateTime localDateTime = dateTimeList.get(i);
                    statement2.execute(String.format("alter fileStorage '%s' as of timestamp '%s'",
                        engine.name(),
                        String.format("%04d-%02d-%02d %02d:%02d:%02d",
                            localDateTime.getYear(),
                            localDateTime.getMonthValue(),
                            localDateTime.getDayOfMonth(),
                            localDateTime.getHour(),
                            localDateTime.getMinute(),
                            localDateTime.getSecond())));
                    ResultSet resultSet2 = statement2.executeQuery(String.format("select count(*) from %s", tableName));
                    resultSet2.next();
                    int count = resultSet2.getInt(1);
                    Assert.assertTrue(lastCount < count);
                    lastCount = count;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER FILESTORAGE OSS AS OF TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStorageAsOfTimeStamp2() {
        try (Connection connection = getConnection()) {
            String tableName = "testAlterFileStorageAsOfTimeStamp2";
            FileStorageTestUtil.createFileStorageTableWith10000Rows(tableName, engine, connection);
            LocalDateTime localDateTime = LocalDateTime.now();
            Thread.sleep(1000);

            Statement statement = connection.createStatement();
            statement.execute(String.format("alter filestorage '%s' backup", engine.name()));

            try (Connection connection2 = getPolardbxConnection(testDataBase2)) {
                Statement statement2 = connection2.createStatement();
                statement2.execute(String.format("alter fileStorage '%s' as of timestamp '%s'",
                    engine.name(),
                    String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        localDateTime.getYear(),
                        localDateTime.getMonthValue(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        localDateTime.getMinute(),
                        localDateTime.getSecond())));
                long count1 = FileStorageTestUtil.count(connection, tableName);
                Assert.assertTrue(count1 == 10000);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER TABLE table_name PURGE BEFORE TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStoragePurgeBeforeTimeStamp2() {
        try (Connection connection = getConnection()) {

            String tableName = "testAlterFileStoragePurgeBeforeTimeStamp2";
            FileStorageTestUtil.createFileStorageTableWith10000Rows(tableName, engine, connection);

            Statement statement = connection.createStatement();
            statement.executeUpdate("drop table " + tableName);

            Thread.sleep(1000);
            LocalDateTime localDateTime = LocalDateTime.now();

            ResultSet rs = statement.executeQuery("show recyclebin");
            rs.next();
            Assert.assertTrue(rs.getString("ORIGINAL_NAME").equalsIgnoreCase(tableName));
            Assert.assertFalse(rs.next());

            try (Connection connection2 = getPolardbxConnection(testDataBase2)) {
                Statement statement2 = connection2.createStatement();
                statement2.execute(String.format(
                    "/*+TDDL:cmd_extra(BACKUP_OSS_PERIOD=0)*/ alter fileStorage '%s' purge before timestamp '%s'",
                    engine.name(),
                    String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        localDateTime.getYear(),
                        localDateTime.getMonthValue(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        localDateTime.getMinute(),
                        localDateTime.getSecond())));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            rs = statement.executeQuery("show recyclebin");
            Assert.assertFalse(rs.next());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * ALTER FILESTORAGE OSS AS OF TIMESTAMP timestamp_value
     */
    @Test
    public void testAlterFileStorageAsOfTimeStamp4() {
        try (Connection connection = getConnection()) {
            LocalDateTime localDateTime = LocalDateTime.now();
            Thread.sleep(1000);
            String tableName = "testAlterFileStorageAsOfTimeStamp4";
            FileStorageTestUtil.createFileStorageTableWith10000Rows(tableName, engine, connection);

            Statement statement = connection.createStatement();
            statement.execute(String.format("alter filestorage '%s' backup", engine.name()));

            try (Connection connection2 = getPolardbxConnection(testDataBase2)) {
                Statement statement2 = connection2.createStatement();
                statement2.execute(String.format("alter fileStorage '%s' as of timestamp '%s'",
                    engine.name(),
                    String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        localDateTime.getYear(),
                        localDateTime.getMonthValue(),
                        localDateTime.getDayOfMonth(),
                        localDateTime.getHour(),
                        localDateTime.getMinute(),
                        localDateTime.getSecond())));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

            Assert.assertTrue(FileStorageTestUtil.count(connection, tableName) == 0);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testDropFileStorageLocalDisk() {
        initLocalDisk();
        try (Connection connection = getConnection()) {
            String localDiskTableName = "testDropFileStorageLocalDisk";
            FileStorageTestUtil.createFileStorageTableWith10000Rows(localDiskTableName, engine, connection);
            Statement statement = connection.createStatement();
            statement.execute("drop table " + localDiskTableName);
            FileStorageTestUtil.createFileStorageTableWith10000Rows(localDiskTableName, engine, connection);
            statement.execute("Drop FileStorage 'local_disk'");
            Thread.sleep(5000);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            initLocalDisk();
        }
    }
}

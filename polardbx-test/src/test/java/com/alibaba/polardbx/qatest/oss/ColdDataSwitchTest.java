package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Simulating turning on/off cold-data storage by user on the console.
 */
public class ColdDataSwitchTest extends BaseTestCase {
    final private static String testDataBase = "coldDataSwitchDatabase";

    final private static Engine engine = PropertiesUtil.engine();

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @Before
    public void initTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            clearColdDataStatus();
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
            stmt.execute(String.format("create database %s mode = 'auto'", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @After
    public void clearTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            clearColdDataStatus();
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testTurnOffAndOnColdData() {
        String innoTableName = "testInno_1";
        String ossTableName = "testOss_1";
        try (Connection connection = getConnection()) {
            FileStorageTestUtil.prepareInnoTable(connection, innoTableName, 2000, false);

            turnOffColdData();
            Thread.sleep(3000);
            // check oss write failed
            JdbcUtil.executeUpdateFailed(
                connection,
                String.format("create table %s like %s engine='%s' archive_mode='loading'",
                    ossTableName, innoTableName, engine.name()),
                "Data archiving feature is not enabled. Please enable this feature on the console."
            );

            turnOnColdData();
            Thread.sleep(3000);
            // check oss write
            JdbcUtil.executeSuccess(
                connection,
                String.format("create table %s like %s engine='%s' archive_mode='loading'",
                    ossTableName, innoTableName, engine.name())
            );
            // check oss read
            Assert.assertTrue(
                FileStorageTestUtil.count(connection, ossTableName)
                    == FileStorageTestUtil.count(connection, innoTableName)
            );
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testTurnOnAndOffColdData() {
        String innoTableName = "testInno_2";
        String ossTableName = "testOss_2";
        try (Connection connection = getConnection()) {
            FileStorageTestUtil.prepareInnoTable(connection, innoTableName, 2000, false);

            turnOnColdData();
            Thread.sleep(3000);
            // check oss write
            JdbcUtil.executeSuccess(
                connection,
                String.format("create table %s like %s engine='%s' archive_mode='loading'",
                    ossTableName, innoTableName, engine.name())
            );
            // check oss read
            Assert.assertTrue(
                FileStorageTestUtil.count(connection, ossTableName)
                    == FileStorageTestUtil.count(connection, innoTableName)
            );

            turnOffColdData();
            Thread.sleep(3000);
            // check oss write failed
            JdbcUtil.executeUpdateFailed(
                connection,
                String.format("create table %s like %s engine='%s' archive_mode='loading'",
                    ossTableName, innoTableName, engine.name()),
                "Data archiving feature is not enabled. Please enable this feature on the console."
            );
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}

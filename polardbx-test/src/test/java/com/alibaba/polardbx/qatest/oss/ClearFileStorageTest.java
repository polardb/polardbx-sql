package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Test for "CLEAR FILESTORAGE OSS" command
 */
public class ClearFileStorageTest extends BaseTestCase {
    final private static String testDataBase = "clearFileStorageDatabase";

    final private static Engine engine = PropertiesUtil.engine();

    public ClearFileStorageTest() {

    }

    private Connection getConnection() {
        return getPolardbxConnection(testDataBase);
    }

    @Before
    public void initTestDatabase() {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
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
            stmt.execute(String.format("drop database if exists %s ", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void testClearFileStorage() {
        try (Connection conn = getConnection()) {
            String tableName = "testClearFileStorageTable";
            // prepare and check cold data table
            FileStorageTestUtil.createFileStorageTableWith10000Rows(tableName, engine, conn);
            Assert.assertTrue(FileStorageTestUtil.count(conn, tableName) == 10000);

            Statement statement = conn.createStatement();
            // clear filestorage
            statement.execute(String.format("CLEAR FILESTORAGE '%s'", engine.name()));

            // check if the table has been dropped
            ResultSet resultSet = statement.executeQuery(String.format("show tables like '%s'", tableName));
            Assert.assertTrue(!resultSet.next(), "The table hasn't been dropped by 'clear filestorage'");

            // check if the meta is cleared
            resultSet = statement.executeQuery(
                String.format("select count(*) from metadb.files where engine='%s'", engine.name()));
            resultSet.next();
            Assert.assertTrue(resultSet.getLong(1) == 0, "The files meta was not cleared");

            // check if the file storage still exists
            boolean stillExists = false;
            resultSet = statement.executeQuery("show filestorage");
            while (resultSet.next()) {
                if (engine.name().equalsIgnoreCase(resultSet.getString("ENGINE"))) {
                    stillExists = true;
                }
            }
            Assert.assertTrue(stillExists, "The file storage was wrongly dropped");

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}

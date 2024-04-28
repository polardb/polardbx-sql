package com.alibaba.polardbx.gms.metadb;

import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarDataConsistencyLockAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarDataConsistencyLockRecord;
import com.alibaba.polardbx.gms.topology.NodeInfoAccessor;
import com.alibaba.polardbx.gms.topology.NodeInfoRecord;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ColumnarAccessorTest {

    protected static MetaDbDataSource metaDbDataSource;

    // use config in server.properties
    public static final String CONFIG_PATH = System.getProperty("user.dir")
        + "/../polardbx-server/src/main/resources/server.properties";

    private static Properties parseProperties(String path) {
        Properties serverProps = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(path);
            if (in != null) {
                serverProps.load(in);
            }
            return serverProps;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    static {
        try {
            // load server.properties
            Properties properties = parseProperties(CONFIG_PATH);

            // initialize meta db datasource
            System.setProperty("instanceId", "polar-db-x-test");
            metaDbDataSource = new MetaDbDataSource(
                properties.getProperty("metaDbAddr"),
                properties.getProperty("metaDbName"),
                "connProperties=useServerPrepStmts=true&useUnicode=true&characterEncoding=utf-8&useSSL=false",
                properties.getProperty("metaDbUser"),
                properties.getProperty("metaDbPasswd")
            );
            metaDbDataSource.init();
        } catch (Throwable e) {
            throw e;
        }
    }

    @Test
    @Ignore("local test")
    public void test() throws Exception {
        Connection connection = null;
        try {
            connection = metaDbDataSource.getConnection();
            connection.setAutoCommit(false);

//            doTest(connection);
//            doDataConsistencyLockTest(connection);
            doNodeInfoAccessorTest(connection);

            // must roll back to prevent from pollution of meta db.
            connection.rollback();
        } catch (Exception e) {
            try {
                if (null != connection) {
                    connection.rollback();
                }
            } catch (SQLException ignored) {
            }

            throw e;
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(true);
                    connection.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private void doTest(Connection connection) {
        // checkpoints write
        ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
        checkpointsAccessor.setConnection(connection);

        List<ColumnarCheckpointsRecord> checkpointsRecords = new ArrayList<>();
        ColumnarCheckpointsRecord checkpointsRecord = new ColumnarCheckpointsRecord();
        checkpointsRecord.logicalSchema = "s1";
        checkpointsRecord.checkpointTso = 1000_000L;
        checkpointsRecord.checkpointType = "stream";
        checkpointsRecord.offset = "{\"server_id\": \"1\", \"pos\": 10}";
        checkpointsRecords.add(checkpointsRecord);

        checkpointsAccessor.insert(checkpointsRecords);

        // checkpoints read
        List<ColumnarCheckpointsRecord> checkpointsResults =
            checkpointsAccessor.queryByLastTso(1000_000L - 1, "s1");

        // Assert
        Assert.assertEquals(checkpointsRecords.size(), checkpointsResults.size());

        // appended file write
        ColumnarAppendedFilesAccessor appendedFilesAccessor = new ColumnarAppendedFilesAccessor();
        appendedFilesAccessor.setConnection(connection);

        List<ColumnarAppendedFilesRecord> appendedFilesRecords = new ArrayList<>();
        ColumnarAppendedFilesRecord appendedFilesRecord = new ColumnarAppendedFilesRecord();
        appendedFilesRecord.checkpointTso = 999_999L;
        appendedFilesRecord.logicalSchema = "s1";
        appendedFilesRecord.logicalTable = "t1";
        appendedFilesRecord.physicalSchema = "ps1";
        appendedFilesRecord.physicalTable = "pt1";
        appendedFilesRecord.partName = "p0";
        appendedFilesRecord.fileName = "file1";
        appendedFilesRecord.engine = "OSS";
        appendedFilesRecord.fileType = "csv";
        appendedFilesRecord.fileLength = 1000_000;
        appendedFilesRecord.appendOffset = 1000_000 - 100;
        appendedFilesRecord.appendLength = 100;
        appendedFilesRecords.add(appendedFilesRecord);

        appendedFilesAccessor.insert(appendedFilesRecords);

        // read
        List<ColumnarAppendedFilesRecord> appendedFilesResults = appendedFilesAccessor.queryByTso(
            0L, "s1", "t1", "p0");

        // Assert
        Assert.assertEquals(appendedFilesRecords.size(), appendedFilesResults.size());

        System.out.println(appendedFilesRecord);
        System.out.println(checkpointsRecord);
    }

    private void doDataConsistencyLockTest(Connection connection) {
        final ColumnarDataConsistencyLockAccessor accessor = new ColumnarDataConsistencyLockAccessor();
        accessor.setConnection(connection);

        final ColumnarDataConsistencyLockRecord record = new ColumnarDataConsistencyLockRecord();
        record.entityId = "testdb.testtb.testpt";
        record.ownerId = ColumnarDataConsistencyLockRecord.OWNER_TYPE.STREAM.getOwnerId();
        record.state = 1;
        record.lastOwner = ColumnarDataConsistencyLockRecord.OWNER_TYPE.STREAM.getOwnerId();
        accessor.insert(record);

        try {
            final List<ColumnarDataConsistencyLockRecord> result = accessor.query(record.entityId);
            Assert.assertEquals(1, result.size());
            final ColumnarDataConsistencyLockRecord record1 = result.get(0);
            Assert.assertEquals(record.entityId, record1.entityId);
            Assert.assertEquals(record.ownerId, record1.ownerId);
            Assert.assertEquals(record.state, record1.state);
            Assert.assertEquals(record.lastOwner, record1.lastOwner);

            System.out.println(record1);
        } finally {
        }

        try {
            accessor.updateLockRelease(1, record.entityId, record.ownerId);

            final List<ColumnarDataConsistencyLockRecord> result = accessor.query(record.entityId);
            Assert.assertEquals(1, result.size());
            final ColumnarDataConsistencyLockRecord record1 = result.get(0);
            Assert.assertEquals(record.entityId, record1.entityId);
            Assert.assertEquals(record.ownerId, record1.ownerId);
            Assert.assertEquals(0, record1.state);
            Assert.assertEquals(record.lastOwner, record1.lastOwner);

            System.out.println(record1);
        } finally {
        }

        try {
            accessor.updateLockOwner(1, ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId(),
                record.entityId, record.ownerId);

            final List<ColumnarDataConsistencyLockRecord> result = accessor.query(record.entityId);
            Assert.assertEquals(1, result.size());
            final ColumnarDataConsistencyLockRecord record1 = result.get(0);
            Assert.assertEquals(record.entityId, record1.entityId);
            Assert.assertEquals(ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId(), record1.ownerId);
            Assert.assertEquals(1, record1.state);
            Assert.assertEquals(record.ownerId, record1.lastOwner);

            System.out.println(record1);
        } finally {
        }

        try {
            accessor.updateLockState(1, record.entityId,
                ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId(), 1);

            final List<ColumnarDataConsistencyLockRecord> result = accessor.query(record.entityId);
            Assert.assertEquals(1, result.size());
            final ColumnarDataConsistencyLockRecord record1 = result.get(0);
            Assert.assertEquals(record.entityId, record1.entityId);
            Assert.assertEquals(ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId(), record1.ownerId);
            Assert.assertEquals(2, record1.state);
            Assert.assertEquals(record.ownerId, record1.lastOwner);

            System.out.println(record1);
        } finally {
        }

        try {
            accessor.updateLockRelease(2, record.entityId,
                ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId());

            final List<ColumnarDataConsistencyLockRecord> result = accessor.query(record.entityId);
            Assert.assertEquals(1, result.size());
            final ColumnarDataConsistencyLockRecord record1 = result.get(0);
            Assert.assertEquals(record.entityId, record1.entityId);
            Assert.assertEquals(ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId(), record1.ownerId);
            Assert.assertEquals(0, record1.state);
            Assert.assertEquals(record.ownerId, record1.lastOwner);

            System.out.println(record1);
        } finally {
        }

        try {
            final List<ColumnarDataConsistencyLockRecord> result = accessor.queryForRead(record.entityId,
                ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId());
            Assert.assertEquals(1, result.size());
            final ColumnarDataConsistencyLockRecord record1 = result.get(0);
            Assert.assertEquals(record.entityId, record1.entityId);
            Assert.assertEquals(ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId(), record1.ownerId);
            Assert.assertEquals(0, record1.state);
            Assert.assertEquals(record.ownerId, record1.lastOwner);

            System.out.println(record1);
        } finally {
        }

        try {
            final List<ColumnarDataConsistencyLockRecord> result = accessor.queryForUpdate(record.entityId);
            Assert.assertEquals(1, result.size());
            final ColumnarDataConsistencyLockRecord record1 = result.get(0);
            Assert.assertEquals(record.entityId, record1.entityId);
            Assert.assertEquals(ColumnarDataConsistencyLockRecord.OWNER_TYPE.COMPACTION.getOwnerId(), record1.ownerId);
            Assert.assertEquals(0, record1.state);
            Assert.assertEquals(record.ownerId, record1.lastOwner);

            System.out.println(record1);
        } finally {
        }

        try {
            accessor.clearLock();
            final List<ColumnarDataConsistencyLockRecord> result = accessor.queryForUpdate(record.entityId);
            Assert.assertEquals(0, result.size());
            final List<ColumnarDataConsistencyLockRecord> allResult = accessor.queryAll();
            Assert.assertEquals(0, allResult.size());
        } finally {
        }
    }

    private void doNodeInfoAccessorTest(Connection connection) {
        final NodeInfoAccessor accessor = new NodeInfoAccessor();
        accessor.setConnection(connection);

        try {
            final List<NodeInfoRecord> nodeInfoRecords = accessor.queryLatestActive();
            Assert.assertEquals(1, nodeInfoRecords.size());
        } finally {
        }
    }
}

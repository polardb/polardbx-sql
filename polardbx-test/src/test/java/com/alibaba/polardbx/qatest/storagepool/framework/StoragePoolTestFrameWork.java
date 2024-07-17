package com.alibaba.polardbx.qatest.storagepool.framework;

import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.storagepool.StoragePoolUtils;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.NotThreadSafe.DeadlockTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.aliyun.oss.common.utils.StringUtils;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@NotThreadSafe
public class StoragePoolTestFrameWork extends DDLBaseNewDBTestCase {

    static Long maxWaitTimeout = 30L;

    public Map<Integer, String> storageInstMap = new HashMap<>();

    private static final Log logger = LogFactory.getLog(StoragePoolTestFrameWork.class);

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public void testPreFrameWork(List<StoragePoolTestAction> storagePoolTestActions)
        throws SQLException {
        final List<Connection> connections = new ArrayList<>(2);
        final Connection connection = connections.get(0);
        for (int i = 0; i < 2; i++) {
            connections.add(getPolardbxConnection());
        }
        beforeTest(connection);
        for (StoragePoolTestAction storagePoolTestAction : storagePoolTestActions) {
            storagePoolTestAction.innerTest(connection);
        }
    }

    public void testRunFrameWork(List<StoragePoolTestAction> storagePoolTestActions)
        throws SQLException {
        final List<Connection> connections = new ArrayList<>(2);
        final Connection connection = connections.get(0);
        for (int i = 0; i < 2; i++) {
            connections.add(getPolardbxConnection());
        }
        try {
            for (StoragePoolTestAction storagePoolTestAction : storagePoolTestActions) {
                storagePoolTestAction.innerTest(connection);
            }
//            createDatabase(tableName, single, createTableStmt);
//            innerTest(tableName, connections, ddlStmt);
        } finally {
            clearTest(connection);
        }
    }

    private void clearTest(Connection connection) {
        String queryDatabase = "show databases";
        List<String> databases = JdbcUtil.executeQueryAndGetColumnResult(queryDatabase, connection, 1);
        checkStoragePoolDnList(connection);
        List<String> createdDatabases =
            databases.stream().filter(o -> !o.equalsIgnoreCase(InformationSchema.NAME)).collect(
                Collectors.toList());
        for (String database : createdDatabases) {
            String dropDatabaseSql = "drop database " + database;
            JdbcUtil.executeUpdateSuccess(connection, dropDatabaseSql);
            checkStoragePoolDnList(connection);
        }

        String queryStoragePoolSql = " select * from information_schema.storage_pool_info";
        List<String> storagePools = JdbcUtil.executeQueryAndGetColumnResult(queryStoragePoolSql, connection, "name");
        List<String> createdStoragePools = storagePools.stream().filter(
                o -> !(o.equalsIgnoreCase(StoragePoolUtils.RECYCLE_STORAGE_POOL) || o.equalsIgnoreCase(
                    StoragePoolUtils.DEFAULT_STORAGE_POOL)))
            .collect(Collectors.toList());
        for (String storagePool : createdStoragePools) {
            String dropStoragePoolSql = "drop storage pool " + storagePool;
            JdbcUtil.executeUpdateSuccess(connection, dropStoragePoolSql);
            checkStoragePoolDnList(connection);
        }
    }

    private void checkStoragePoolDnList(Connection connection) {
        String queryStoragePoolSql = " select * from information_schema.storage_pool_info";
        List<String> storagePools = JdbcUtil.executeQueryAndGetColumnResult(queryStoragePoolSql, connection, "NAME");
        List<String> storagePoolDnIds =
            JdbcUtil.executeQueryAndGetColumnResult(queryStoragePoolSql, connection, "DN_ID_LIST");
        List<String> undeletableDns =
            JdbcUtil.executeQueryAndGetColumnResult(queryStoragePoolSql, connection, "UNDELETABLE_DN_ID");

        Set<String> storagePoolSet = storagePools.stream().collect(Collectors.toSet());
        String errorMsg = "";
        if (storagePools.size() != storagePoolSet.size()) {
            errorMsg = StringUtils.join(",", storagePools);
            Assert.fail("There are duplicate storage pool name !" + errorMsg);
        }
        List<String> dnIds =
            storagePoolDnIds.stream().map(o -> o.split(",")).flatMap(Arrays::stream).collect(Collectors.toList());
        Set<String> dnIdSet = dnIds.stream().collect(Collectors.toSet());
        if (dnIds.size() != dnIdSet.size()) {
            errorMsg = StringUtils.join(",", storagePoolDnIds);
            Assert.fail("There are duplicate storage inst !" + errorMsg);
        }
        if (storageInstMap.size() != dnIdSet.size()) {
            errorMsg = StringUtils.join(",", storagePoolDnIds);
            Assert.fail("There lacks some storage inst !" + errorMsg);

        }

        for (int i = 0; i < storagePools.size(); i++) {
            String storagePool = storagePools.get(i);
            String storageDnId = storagePoolDnIds.get(i);
            String undeletableDnId = undeletableDns.get(i);
            if (storagePool.equalsIgnoreCase(StoragePoolUtils.RECYCLE_STORAGE_POOL)) {
                if (StringUtils.isNullOrEmpty(undeletableDnId)) {
                    Assert.fail("recylce storage inst undeletableDnId should be empty, but now is !" + undeletableDnId);
                }
            } else if (storagePool.equalsIgnoreCase(StoragePoolUtils.DEFAULT_STORAGE_POOL)) {
                if (StringUtils.isNullOrEmpty(undeletableDnId) || StringUtils.isNullOrEmpty(storageDnId)) {
                    Assert.fail("default storage inst undeletableDnId and storageIds should not be empty, but now is !"
                        + undeletableDnId + "-" + storageDnId);
                }
                if (!storageDnId.contains(undeletableDnId)) {
                    Assert.fail(String.format("storage pool %s: storageIds is %s, while undeletableDnId is %s",
                        storagePool, storageDnId, undeletableDnId));
                }
            } else {
                if (!storageDnId.contains(undeletableDnId)) {
                    Assert.fail(String.format("storage pool %s: storageIds is %s, while undeletableDnId is %s",
                        storagePool, storageDnId, undeletableDnId));
                }
            }
        }
    }

    private Map<String, String> constructMapFromTwoList(List<String> l1, List<String> l2) {
        Map<String, String> results = new HashMap<>();
        for (int i = 0; i < l1.size(); i++) {
            results.put(l1.get(i), l2.get(i));
        }
        return results;
    }

    public void beforeTest(Connection connection) {
        String queryDatabase = "show databases";
        List<String> databases = JdbcUtil.executeQueryAndGetColumnResult(queryDatabase, connection, 1);
        List<String> createdDatabases =
            databases.stream().filter(o -> o.equalsIgnoreCase(InformationSchema.NAME)).collect(
                Collectors.toList());
        String queryStorage = "show storage where inst_kind = 'MASTER' order by deletable";
        List<String> storageInsts =
            JdbcUtil.executeQueryAndGetColumnResult(queryStorage, connection, "STORAGE_INST_ID");
        for (int i = 0; i < storageInsts.size(); i++) {
            storageInstMap.put(i, storageInsts.get(i));
        }
        if (!createdDatabases.isEmpty()) {
            Assert.fail("Alter Storage Pool Test: Must be an empty inst!");
        }
    }

    public void innerTest(String tableName, List<Connection> connections, String ddl) {

//        String sql = "insert into " + tableName + " values (0), (1)";
//        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        // Connection 0: select for update
        JdbcUtil.executeQuerySuccess(connections.get(0), "begin");
        String sql = "select * from " + tableName + "  for update";
        JdbcUtil.executeQuerySuccess(connections.get(0), sql);

        final ExecutorService threadPool = new ThreadPoolExecutor(1, 1, 0L,
            TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory(StoragePoolTestFrameWork.class.getSimpleName(), false));
        final List<Future<Boolean>> futures = new LinkedList<>();

        // Connection 1: ddl
        futures.add(executeSqlAndCommit(threadPool, tableName, connections.get(1), ddl));

        for (Future<Boolean> future : futures) {
            try {
                if (!future.get(maxWaitTimeout, TimeUnit.SECONDS)) {
                    Assert.fail("Mdl Detection: Logical ddl failed!");
                }
            } catch (TimeoutException e) {
                e.printStackTrace();
                Assert.fail("Mdl Detection: Wait for too long, more than maxWaitTimeout seconds in !" + getClass());
            } catch (Exception e) {
                Assert.fail("Mdl Detection: failed for unexpected cause!");
            }
        }
    }

    public Future<Boolean> executeSqlAndCommit(ExecutorService threadPool, String tableName,
                                               Connection connection, String sql) {
        return threadPool.submit(() -> {
            try {
                JdbcUtil.executeUpdate(connection, sql);
            } catch (Throwable e) {
                return false;
            }
            return true;
        });
    }
}



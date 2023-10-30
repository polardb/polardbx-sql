package com.alibaba.polardbx.qatest.storagepool.base;

import com.alibaba.polardbx.druid.util.JdbcUtils;
import com.alibaba.polardbx.qatest.storagepool.framework.StoragePoolTestAction;
import com.alibaba.polardbx.qatest.storagepool.framework.StoragePoolTestFrameWork;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@NotThreadSafe
public class StoragePoolMetaTest {
    private static final Log logger = LogFactory.getLog(StoragePoolMetaTest.class);

//    @Test
//    public void testCreateStoragePool() throws SQLException {
//        StoragePoolTestFrameWork storagePoolTestFrameWork = new StoragePoolTestFrameWork();
//        storagePoolTestFrameWork.testPreFrameWork(new ArrayList<>());
//        Map<Integer, String> storageInstMap = storagePoolTestFrameWork.storageInstMap;
//
//        int dnNums = storageInstMap.size();
//        String createDatabaseSql = "create database old_auto_database mode = auto;";
//        String drainNodeSql =
//            String.format("rebalance database old_auto_database drain_node = '%s';", storageInstMap.get(dnNums - 1));
//        String markDnSql = String.format("update metadb.storage_info set status = 0;");
//        List<String> createStoragePoolSqls = new ArrayList<>();
//        for (int i = 0; i < 3; i++) {
//            String createStoragePoolSql = String.format("create storage pool sp%d dn_list='%s,%s'",
//                i, storageInstMap.get((i + 1) * 2), storageInstMap.get((i + 1) * 2 + 1));
//            createStoragePoolSqls.add(createStoragePoolSql);
//        }
//        StoragePoolTestAction storagePoolTestAction = new StoragePoolTestAction() {
//            @Override
//            public void innerTest(Connection connection) {
//                JdbcUtil.executeUpdateSuccess(connection, createDatabaseSql);
//                logger.debug(" execute " + createDatabaseSql);
//
//                JdbcUtil.executeUpdateSuccess(connection, drainNodeSql);
//                logger.debug(" execute " + drainNodeSql);
//
//                JdbcUtil.executeUpdateSuccess(connection, markDnSql);
//                logger.debug(" execute " + markDnSql);
//
//                for (String createStoragePoolSql : createStoragePoolSqls) {
//                    JdbcUtil.executeUpdateSuccess(connection, createStoragePoolSql);
//                    logger.debug(" execute " + createStoragePoolSql);
//                }
//            }
//        };
//        storagePoolTestFrameWork.testRunFrameWork(Arrays.asList(storagePoolTestAction));
//    }
//
}

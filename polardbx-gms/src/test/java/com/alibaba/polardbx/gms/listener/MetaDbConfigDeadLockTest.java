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

package com.alibaba.polardbx.gms.listener;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.gms.topology.ConfigListenerRecord;
import com.google.common.collect.Maps;
import org.checkerframework.checker.units.qual.A;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chenghui.lch
 */
@Ignore
public class MetaDbConfigDeadLockTest {

    private static final Logger logger = LoggerFactory.getLogger(MetaDbConfigDeadLockTest.class);

    protected final static String addr = "127.0.0.1:3306";
    protected final static String dbName = "polardbx_meta_db_polardbx";
    protected final static String props = "useUnicode=true&characterEncoding=utf-8&useSSL=false";
    protected final static String usr = "diamond";
    protected final static String pwd = "STA0HLmViwmos2woaSzweB9QM13NjZgrOtXTYx+ZzLw=";

    protected static MetaDbDataSource metaDbDataSource = null;
    protected static Connection connection = null;

    static {
        try {
            MetaDbDataSource.initMetaDbDataSource(addr, dbName, props, usr, pwd);
            connection = MetaDbDataSource.getInstance().getConnection();
            if (connection != null) {
                System.out.println("init metadb");
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    final static String data_id="data.id.test";
    final static AtomicBoolean isStop = new AtomicBoolean(false);
    public static class  RegisterTask  implements Callable<Void> {
        @Override
        public Void call() throws Exception {
            while (!isStop.get()) {
                try {
                    addDataIdInfoIntoDb(data_id, null);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                    logger.error(ex);
                }

            }
            return null;
        }

        protected ConfigListenerRecord addDataIdInfoIntoDb(String dataId, Connection metaDbConn) {
            try {
                ConfigListenerRecord dataIdInfo = null;
                if (metaDbConn == null) {
                    try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
                        ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                        configListenerAccessor.setConnection(conn);
                        try {
                            conn.setAutoCommit(false);

                            // add dataId into metaDB
                            while (true) {
                                try {

                                    dataIdInfo = configListenerAccessor.getDataId(dataId, true);
                                    if (dataIdInfo != null && dataIdInfo.status == ConfigListenerRecord.DATA_ID_STATUS_NORMAL) {
                                        // dataId has already exists, so ignore

                                        // For test
                                        try {
                                            Thread.sleep(500);
                                        } catch (Throwable ex) {
                                            //
                                        }

                                        break;
                                    }

                                    configListenerAccessor
                                        .addDataId(dataId, ConfigListenerRecord.DATA_ID_STATUS_NORMAL,
                                            ConfigListenerAccessor.DEFAULT_OP_VERSION);

                                   //For Test
                                    try {
                                        Thread.sleep(500);
                                    } catch (Throwable ex) {
                                        //
                                    }

                                    break;
                                } catch (Exception e) {
                                    if (e.getMessage().toLowerCase().contains("deadlock found")) {
                                        Random rnd = new Random();
                                        int randWaitTime = Math.abs(rnd.nextInt(500));
                                        try {
                                            Thread.sleep(randWaitTime);
                                        } catch (Throwable ex) {
                                            //
                                        }
                                        dataIdInfo = configListenerAccessor.getDataId(dataId, false);
                                        if (dataIdInfo != null) {
                                            break;
                                        }
                                    } else {
                                        throw e;
                                    }
                                }
                            }
                            conn.commit();

                            // Select only
                            if (dataIdInfo == null) {
                                dataIdInfo = configListenerAccessor.getDataId(dataId, false);
                            }
                            return dataIdInfo;
                        } catch (Throwable e) {
                            conn.rollback();
                            throw e;
                        } finally {
                            conn.setAutoCommit(true);
                        }
                    } catch (Throwable ex) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format("Failed to add dataId[%s] into metaDb", dataId), ex);
                    }
                } else {
                    ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
                    configListenerAccessor.setConnection(metaDbConn);
                    dataIdInfo = configListenerAccessor.getDataId(dataId, true);
                    if (dataIdInfo != null && dataIdInfo.status == ConfigListenerRecord.DATA_ID_STATUS_NORMAL) {
                        return dataIdInfo;
                    }
                    configListenerAccessor
                        .addDataId(dataId, ConfigListenerRecord.DATA_ID_STATUS_NORMAL,
                            ConfigListenerAccessor.DEFAULT_OP_VERSION);
                    // Select and lock after insert
                    dataIdInfo = configListenerAccessor.getDataId(dataId, true);
                    return dataIdInfo;
                }

            } catch (Throwable ex) {
                throw ex;
            }
        }
    }


    @Test
    public void testDeadLock() throws Exception {

        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final List<Callable<Void>> tasks = new ArrayList<>();



        tasks.add(new RegisterTask());
        tasks.add(new RegisterTask());
        tasks.add(new RegisterTask());
        tasks.add(new RegisterTask());

        tasks.add(() -> {
            while (!isStop.get()) {
                try {
                    MetaDbConfigManager.getInstance().unregister(data_id,null);
                } catch (Throwable ex) {
                    logger.warn(ex);
                }
            }
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        long time = 120000;
        Thread.sleep(time);
        isStop.set(true);

        for (Future<Void> result : results) {
            result.get();
        }


    }

    //@Test
    public void tesConfigListenerManager() {

        try {
            MetaDbConfigManager manager = MetaDbConfigManager.getInstance();

            String testDataId1 = "test.data_id.1";
            String testDataId2 = "test.data_id.2";

            Map<String, List<Integer>> listenerRsMap = Maps.newHashMap();
            listenerRsMap.put(testDataId1, new ArrayList<>());
            listenerRsMap.put(testDataId2, new ArrayList<>());

            manager.bindListener(testDataId1, new ConfigListener() {
                @Override
                public void onHandleConfig(String dataId, long newOpVersion) {
                    List<Integer> rsList = listenerRsMap.get(dataId);
                    rsList.add(1);
                }
            });

            manager.bindListener(testDataId2, new ConfigListener() {
                @Override
                public void onHandleConfig(String dataId, long newOpVersion) {
                    List<Integer> rsList = listenerRsMap.get(dataId);
                    rsList.add(1);
                }
            });
            manager.init();

            Connection metaDbConn = MetaDbDataSource.getInstance().getConnection();
            ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
            configListenerAccessor.setConnection(metaDbConn);

            int updateTime = 3;
            for (int i = 0; i < updateTime; i++) {

                try {
                    //configListenerAccessor.register(testDataId, initOpVer);
                    long opVer = 0;
                    opVer = configListenerAccessor.updateOpVersion(testDataId1);
                    System.out.println(opVer);
                    opVer = configListenerAccessor.updateOpVersion(testDataId2);
                    System.out.println(opVer);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }

                Thread.sleep(5000);
            }

            Assert.assertTrue(listenerRsMap.get(testDataId1).size() == updateTime);
            Assert.assertTrue(listenerRsMap.get(testDataId2).size() == updateTime);

            configListenerAccessor.deleteDataId(testDataId1);
            configListenerAccessor.deleteDataId(testDataId2);
            metaDbConn.close();
        } catch (Throwable ex) {
            logger.info(ex.getMessage());
            Assert.fail(ex.getMessage());
        }
    }

}

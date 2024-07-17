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

package com.alibaba.polardbx.qatest.ddl.auto.dag;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineResourceManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

@Ignore("这个查询会清理系统表，不允许")
public class DdlEngineResourceManagerTest {

    DdlEngineResourceManager resourceManager = new DdlEngineResourceManager();

    private PersistentReadWriteLock rwLockManager = PersistentReadWriteLock.create();

    private static String schemaName = "DdlEngineResourceManagerTest";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
        String addr =
            ConnectionManager.getInstance().getMetaAddress() + ":" + ConnectionManager.getInstance().getMetaPort();
        String dbName = PropertiesUtil.getMetaDB;
        String props = "useUnicode=true&characterEncoding=utf-8&useSSL=false";
        String usr = ConnectionManager.getInstance().getMetaUser();
        String pwd = PropertiesUtil.configProp.getProperty(ConfigConstant.META_PASSWORD);
        MetaDbDataSource.initMetaDbDataSource(addr, dbName, props, usr, pwd);
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + schemaName);
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                "create database " + schemaName);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + schemaName);
        }
    }

    @Test
    public void testAcquireAndRelease() {
        resourceManager.acquireResource(schemaName, 1L, Sets.newHashSet("aa"), Sets.newHashSet("bb", "cc"));
        resourceManager.acquireResource(schemaName, 2L, Sets.newHashSet("aa"), Sets.newHashSet("dd", "ee"));
        resourceManager.releaseResource(1L);
        resourceManager.acquireResource(schemaName, 2L, Sets.newHashSet("aa"), Sets.newHashSet("aa"));
        resourceManager.releaseResource(2L);
        resourceManager.acquireResource(schemaName, 3L, Sets.newHashSet("aa"), Sets.newHashSet("aa", "bb"));
        resourceManager.releaseResource(3L);
    }

    @Test
    public void testJobDisappear() {
        resourceManager.acquireResource(schemaName, 1L, Sets.newHashSet("a"), Sets.newHashSet("b", "c"));
        resourceManager.acquireResource(schemaName, 2L, Sets.newHashSet("a"), Sets.newHashSet("b", "c"));
        resourceManager.releaseResource(2L);
    }

    @Test
    public void testJobFailedOnLegacyEngine() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.GMS);
        try (Connection metaConn = MetaDbUtil.getConnection()) {
            JdbcUtil.executeUpdateSuccess(metaConn, "delete from ddl_jobs where job_id=1346234132807548928");
            JdbcUtil
                .executeUpdateSuccess(metaConn, "delete from read_write_lock where owner='DDL_1346234132807548928'");
            String sql =
                "INSERT INTO `ddl_jobs`(`id`, `schema_name`, `job_id`, `parent_job_id`, `job_no`, `server`, `object_schema`, `object_name`, `new_object_name`, `job_type`, `phase`, `state`, `physical_object_done`, `progress`, `ddl_stmt`, `old_rule_text`, `new_rule_text`, `gmt_created`, `gmt_modified`, `remark`, `reserved_gsi_int`, `reserved_gsi_txt`, `reserved_ddl_int`, `reserved_ddl_txt`, `reserved_cmn_int`, `reserved_cmn_txt`) VALUES (85, 'd1', 1346234132807548928, 0, 0, '0:127.0.0.1:8527:12aec8cff2000000', 'd1', 't2', '', 'CREATE_TABLE', 'EXECUTE', 'PENDING', 'D1_000001_GROUP:`t2_8SJe`:--;D1_000003_GROUP:`t2_8SJe`:--;D1_000002_GROUP:`t2_8SJe`:--;D1_000000_GROUP:`t2_8SJe`:--', 100, '/*+TDDL:cmd_extra(FORCE_DDL_ON_LEGACY_ENGINE=TRUE)*/create table t2(     c1 bigint primary key auto_increment by group,     c2 bigint,     c3 bigint ) dbpartition by hash(c1)', NULL, NULL, 1624862889585, 1624862890962, 'injected failure', NULL, NULL, 0, NULL, NULL, NULL)";
            JdbcUtil.executeUpdateSuccess(metaConn, sql);
        } catch (Exception e) {

        }
        try {
            resourceManager
                .acquireResource(schemaName, 1346234132807548928L, Sets.newHashSet("a"), Sets.newHashSet("b", "c"));
            resourceManager
                .acquireResource(schemaName, 2L, Sets.newHashSet("a"), Sets.newHashSet("b", "c"));
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("ERR_PAUSED_DDL_JOB_EXISTS"));
            resourceManager
                .releaseResource(1346234132807548928L);
        }
    }

    @Test
    public void testJobFailedOnNewEngine() {
        ConfigDataMode.setMode(ConfigDataMode.Mode.GMS);
        try (Connection metaConn = MetaDbUtil.getConnection()) {
            JdbcUtil.executeUpdateSuccess(metaConn, "delete from ddl_engine where job_id=1344845353727295488");
            JdbcUtil
                .executeUpdateSuccess(metaConn, "delete from read_write_lock where owner='DDL_1342195167473434624'");
            String sql =
                "INSERT INTO `ddl_engine`(`id`, `job_id`, `ddl_type`, `schema_name`, `object_name`, `response_node`, `execution_node`, `state`, `resources`, `progress`, `trace_id`, `context`, `task_graph`, `result`, `ddl_stmt`, `gmt_created`, `gmt_modified`, `max_parallelism`, `supported_commands`, `paused_policy`, `rollback_paused_policy`) VALUES (67, 1344845353727295488, 'CREATE_TABLE', 'fail_point', 'gsi_primary_table', '127.0.0.1:8527', '127.0.0.1:8527', 'PAUSED', 'fail_point.gsi_primary_table', 100, '12a9d9b932400000', '{\\\"@type\\\":\\\"com.taobao.tddl.optimizer.context.DdlContext\\\",\\\"asyncMode\\\":false,\\\"beingRolledBack\\\":false,\\\"dataPassed\\\":{\\\"@type\\\":\\\"java.util.HashMap\\\",\\\"TEST_MODE\\\":false,\\\"CLIENT_IP\\\":\\\"127.0.0.1\\\",\\\"TRACE_ID\\\":\\\"12a9d9b932400000\\\",\\\"CONNECTION_ID\\\":7L,\\\"TX_ID\\\":1344845353182035968},\\\"ddlStmt\\\":\\\"/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true)*/ CREATE TABLE IF NOT EXISTS `gsi_primary_table` (\\\\n\\\\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\\\\n\\\\t`c2` int(20) DEFAULT NULL,\\\\n\\\\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\\\\n\\\\t`sint` smallint(6) DEFAULT \\'1000\\',\\\\n\\\\t`mint` mediumint(9) DEFAULT NULL,\\\\n\\\\t`bint` bigint(20) DEFAULT NULL COMMENT \\' bigint\\',\\\\n\\\\t`dble` double(10, 2) DEFAULT NULL,\\\\n\\\\t`fl` float(10, 2) DEFAULT NULL,\\\\n\\\\t`dc` decimal(10, 2) DEFAULT NULL,\\\\n\\\\t`num` decimal(10, 2) DEFAULT NULL,\\\\n\\\\t`dt` date DEFAULT NULL,\\\\n\\\\t`ti` time DEFAULT NULL,\\\\n\\\\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\\\\n\\\\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\\\\n\\\\t`dti` datetime DEFAULT NULL,\\\\n\\\\t`vc` varchar(100) COLLATE utf8_bin DEFAULT NULL,\\\\n\\\\t`vc2` varchar(100) COLLATE utf8_bin NOT NULL,\\\\n\\\\t`tb` tinyblob,\\\\n\\\\t`bl` blob,\\\\n\\\\t`mb` mediumblob,\\\\n\\\\t`lb` longblob,\\\\n\\\\t`tt` tinytext COLLATE utf8_bin,\\\\n\\\\t`mt` mediumtext COLLATE utf8_bin,\\\\n\\\\t`lt` longtext COLLATE utf8_bin,\\\\n\\\\t`en` enum(\\'1\\', \\'2\\') COLLATE utf8_bin NOT NULL,\\\\n\\\\t`st` set(\\'5\\', \\'6\\') COLLATE utf8_bin DEFAULT NULL,\\\\n\\\\t`id1` int(11) DEFAULT NULL,\\\\n\\\\t`id2` int(11) DEFAULT NULL,\\\\n\\\\t`id3` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,\\\\n\\\\t`vc1` varchar(100) COLLATE utf8_bin DEFAULT NULL,\\\\n\\\\t`vc3` varchar(100) COLLATE utf8_bin DEFAULT NULL,\\\\n\\\\tPRIMARY KEY (`pk`),\\\\n\\\\tUNIQUE `idx3` USING BTREE (`vc1`(20)),\\\\n\\\\tKEY `idx1` USING HASH (`id1`),\\\\n\\\\tKEY `idx2` USING HASH (`id2`),\\\\n\\\\tFULLTEXT KEY `idx4` (`id3`),\\\\n\\\\tGLOBAL INDEX gsi_id2(id2) COVERING (vc1, vc2) DBPARTITION BY HASH (id2) \\\\n\\\\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin CHECKSUM = 0 COMMENT = \\\\\\\"abcd\\\\\\\" dbpartition BY HASH ( id1 );\\\",\\\"ddlType\\\":\\\"CREATE_TABLE\\\",\\\"enableTrace\\\":false,\\\"extraCmds\\\":{\\\"@type\\\":\\\"java.util.HashMap\\\",\\\"FORCE_APPLY_CACHE\\\":\\\"TRUE\\\",\\\"CONN_POOL_XPROTO_XPLAN_TABLE_SCAN\\\":\\\"false\\\",\\\"ENABLE_SPM\\\":\\\"FALSE\\\",\\\"ENABLE_MPP\\\":\\\"FALSE\\\",\\\"CONN_POOL_XPROTO_CONFIG\\\":\\\"\\\",\\\"CONN_POOL_XPROTO_SESSION_AGING_TIME\\\":\\\"600000\\\",\\\"CONN_POOL_XPROTO_MAX_POOLED_SESSION_PER_INST\\\":\\\"512\\\",\\\"CONN_POOL_XPROTO_PURE_ASYNC_MPP\\\":\\\"true\\\",\\\"RECORD_SQL\\\":\\\"TRUE\\\",\\\"CONN_POOL_XPROTO_MIN_POOLED_SESSION_PER_INST\\\":\\\"32\\\",\\\"CONN_POOL_XPROTO_CHUNK_RESULT\\\":\\\"true\\\",\\\"ENABLE_SCALE_OUT_FEATURE\\\":\\\"true\\\",\\\"SCAN_TIMEOUT_INTERVAL\\\":\\\"121\\\",\\\"SQL_SIMPLE_MAX_LENGTH\\\":\\\"1005\\\",\\\"CHOOSE_STREAMING\\\":true,\\\"ENABLE_DELETE_WITH_LIMIT\\\":\\\"TRUE\\\",\\\"FORBID_EXECUTE_DML_ALL\\\":\\\"FALSE\\\",\\\"COMMIT_TIMEOUT\\\":\\\"180000\\\",\\\"GROUP_SEQ_CHECK_INTERVAL\\\":\\\"5\\\",\\\"CONN_POOL_XPROTO_SLOW_THRESH\\\":\\\"1000\\\",\\\"CONN_POOL_XPROTO_QUERY_TOKEN\\\":\\\"10000\\\",\\\"MPP_RPC_LOCAL_ENABLED\\\":true,\\\"CONN_POOL_MAX_POOL_SIZE\\\":\\\"60\\\",\\\"HINT_PARSER_FLAG\\\":\\\"TRUE\\\",\\\"GET_TSO_TIMEOUT\\\":\\\"5000\\\",\\\"LOGICAL_DB_TIME_ZONE\\\":\\\"+08:00\\\",\\\"STORAGE_SUPPORTS_BLOOM_FILTER\\\":true,\\\"SQL_DELAY_CUTOFF\\\":\\\"2\\\",\\\"CONN_POOL_PROPERTIES\\\":\\\"connectTimeout=5000;characterEncoding=utf8;autoReconnect=true;failOverReadOnly=false;socketTimeout=12000;rewriteBatchedStatements=true;useServerPrepStmts=false;useSSL=false\\\",\\\"ENABLE_SIMPLE_SEQUENCE\\\":\\\"TRUE\\\",\\\"INIT_CONCURRENT_POOL_EVERY_CONNECTION\\\":false,\\\"SINGLE_GROUP_STORAGE_INST_LIST\\\":\\\"polardbx-storage-0-master\\\",\\\"CONN_POOL_XPROTO_MAX_PACKET_SIZE\\\":\\\"67108864\\\",\\\"CONN_POOL_XPROTO_CHECKER\\\":\\\"true\\\",\\\"ENABLE_UPDATE_PARTITION_KEY\\\":\\\"false\\\",\\\"CONN_POOL_XPROTO_DIRECT_WRITE\\\":\\\"false\\\",\\\"CONN_POOL_XPROTO_MAX_SESSION_PER_CLIENT\\\":\\\"1024\\\",\\\"STATISTIC_COLLECTOR_FROM_RULE\\\":true,\\\"CONN_POOL_XPROTO_AUTH\\\":\\\"true\\\",\\\"CONN_POOL_BLOCK_TIMEOUT\\\":\\\"5000\\\",\\\"MPP_TABLESCAN_CONNECTION_STRATEGY\\\":0,\\\"ENABLE_UPDATE_WITH_LIMIT\\\":\\\"TRUE\\\",\\\"ENABLE_CBO\\\":\\\"TRUE\\\",\\\"CONN_POOL_IDLE_TIMEOUT\\\":\\\"60\\\",\\\"CONN_POOL_XPROTO_XPLAN_EXPEND_STAR\\\":\\\"true\\\",\\\"ALLOW_ADD_GSI\\\":\\\"TRUE\\\",\\\"CONN_POOL_XPROTO_STORAGE_DB_PORT\\\":\\\"0\\\",\\\"ENABLE_DDL\\\":\\\"TRUE\\\",\\\"CONN_POOL_XPROTO_PIPE_BUFFER_SIZE\\\":\\\"268435456\\\",\\\"PROCESS_AUTO_INCREMENT_BY_SEQUENCE\\\":true,\\\"ALLOW_SIMPLE_SEQUENCE\\\":\\\"false\\\",\\\"PURGE_TRANS_BEFORE\\\":\\\"604801\\\",\\\"PURGE_TRANS_INTERVAL\\\":\\\"86401\\\",\\\"CONN_POOL_XPROTO_FEEDBACK\\\":\\\"true\\\",\\\"CONN_POOL_XPROTO_MESSAGE_TIMESTAMP\\\":\\\"true\\\",\\\"PARALLELISM\\\":\\\"-1\\\",\\\"STORAGE_CHECK_ON_GSI\\\":\\\"FALSE\\\",\\\"SOCKET_TIMEOUT\\\":\\\"900000\\\",\\\"CONN_POOL_XPROTO_META_DB_PORT\\\":\\\"0\\\",\\\"ENABLE_FORBID_PUSH_DML_WITH_HINT\\\":\\\"FALSE\\\",\\\"ENABLE_MASTER_MPP\\\":false,\\\"MPP_METRIC_LEVEL\\\":0,\\\"CONN_POOL_MIN_POOL_SIZE\\\":\\\"5\\\",\\\"SHARD_DB_COUNT_EACH_STORAGE_INST\\\":\\\"2\\\",\\\"ENABLE_ALTER_DDL\\\":true,\\\"PLAN_EXTERNALIZE_TEST\\\":\\\"TRUE\\\",\\\"CONN_POOL_XPROTO_PLAN_CACHE\\\":\\\"true\\\",\\\"CONN_POOL_XPROTO_MAX_CLIENT_PER_INST\\\":\\\"32\\\",\\\"NET_WRITE_TIMEOUT\\\":28800,\\\"CONN_POOL_XPROTO_XPLAN\\\":\\\"true\\\",\\\"ENABLE_SELF_CROSS_JOIN\\\":true,\\\"INIT_WORKER\\\":\\\"FALSE\\\",\\\"CONN_POOL_XPROTO_TRX_LEAK_CHECK\\\":\\\"false\\\",\\\"CONN_POOL_XPROTO_FLAG\\\":\\\"0\\\",\\\"USE_CHUNK_EXECUTOR\\\":\\\"AUTO\\\",\\\"RETRY_ERROR_SQL_ON_OLD_SERVER\\\":true,\\\"SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE\\\":\\\"false\\\",\\\"MAX_LOGICAL_DB_COUNT\\\":\\\"2048\\\",\\\"CONN_POOL_XPROTO_AUTO_COMMIT_OPTIMIZE\\\":\\\"true\\\"},\\\"extraServerVariables\\\":{\\\"@type\\\":\\\"java.util.HashMap\\\",\\\"pure_async_ddl_mode\\\":false,\\\"sockettimeout\\\":-1,\\\"read\\\":\\\"WRITE\\\",\\\"autocommit\\\":true,\\\"batch_insert_policy\\\":\\\"SPLIT\\\",\\\"sql_mock\\\":false,\\\"transaction policy\\\":3,\\\"trans.policy\\\":\\\"ALLOW_READ\\\",\\\"drds_transaction_policy\\\":\\\"ALLOW_READ\\\"},\\\"jobId\\\":1344845353727295488,\\\"numPhyObjectsDone\\\":0,\\\"numPhyObjectsTotal\\\":0,\\\"objectName\\\":\\\"gsi_primary_table\\\",\\\"phyDdlTaskId\\\":0,\\\"phyObjectsDone\\\":[],\\\"resources\\\":Set[\\\"fail_point.gsi_primary_table\\\"],\\\"schemaName\\\":\\\"fail_point\\\",\\\"serverVariables\\\":{\\\"@type\\\":\\\"java.util.HashMap\\\",\\\"sql_mode\\\":\\\"default\\\",\\\"net_write_timeout\\\":28800L,\\\"time_zone\\\":\\\"+08:00\\\"},\\\"traceId\\\":\\\"12a9d9b932400000\\\",\\\"userDefVariables\\\":{\\\"@type\\\":\\\"java.util.HashMap\\\"},\\\"usingWarning\\\":false}', '{1344845353739878416:[1344845353739878417],1344845353739878417:[1344845353739878418],1344845353739878418:[],1344845353735684096:[1344845353735684097],1344845353739878400:[1344845353739878401],1344845353735684097:[1344845353735684098],1344845353739878401:[1344845353739878402],1344845353739878402:[1344845353739878403],1344845353735684098:[1344845353739878400],1344845353739878403:[1344845353739878404],1344845353739878404:[1344845353739878405],1344845353739878405:[1344845353739878406],1344845353739878406:[1344845353739878407],1344845353739878407:[1344845353739878408],1344845353739878408:[1344845353739878409],1344845353739878409:[1344845353739878410],1344845353739878410:[1344845353739878411],1344845353739878411:[1344845353739878412],1344845353739878412:[1344845353739878413],1344845353739878413:[1344845353739878414],1344845353739878414:[1344845353739878415],1344845353739878415:[1344845353739878416]}', NULL, '/*+TDDL:cmd_extra(STORAGE_CHECK_ON_GSI=false, ALLOW_ADD_GSI=true)*/ CREATE TABLE IF NOT EXISTS `gsi_primary_table` (\\n\t`pk` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\\n\t`c2` int(20) DEFAULT NULL,\\n\t`tint` tinyint(10) UNSIGNED ZEROFILL DEFAULT NULL,\\n\t`sint` smallint(6) DEFAULT \\'1000\\',\\n\t`mint` mediumint(9) DEFAULT NULL,\\n\t`bint` bigint(20) DEFAULT NULL COMMENT \\' bigint\\',\\n\t`dble` double(10, 2) DEFAULT NULL,\\n\t`fl` float(10, 2) DEFAULT NULL,\\n\t`dc` decimal(10, 2) DEFAULT NULL,\\n\t`num` decimal(10, 2) DEFAULT NULL,\\n\t`dt` date DEFAULT NULL,\\n\t`ti` time DEFAULT NULL,\\n\t`tis` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\\n\t`ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\\n\t`dti` datetime DEFAULT NULL,\\n\t`vc` varchar(100) COLLATE utf8_bin DEFAULT NULL,\\n\t`vc2` varchar(100) COLLATE utf8_bin NOT NULL,\\n\t`tb` tinyblob,\\n\t`bl` blob,\\n\t`mb` mediumblob,\\n\t`lb` longblob,\\n\t`tt` tinytext COLLATE utf8_bin,\\n\t`mt` mediumtext COLLATE utf8_bin,\\n\t`lt` longtext COLLATE utf8_bin,\\n\t`en` enum(\\'1\\', \\'2\\') COLLATE utf8_bin NOT NULL,\\n\t`st` set(\\'5\\', \\'6\\') COLLATE utf8_bin DEFAULT NULL,\\n\t`id1` int(11) DEFAULT NULL,\\n\t`id2` int(11) DEFAULT NULL,\\n\t`id3` varchar(100) CHARACTER SET utf8mb4 DEFAULT NULL,\\n\t`vc1` varchar(100) COLLATE utf8_bin DEFAULT NULL,\\n\t`vc3` varchar(100) COLLATE utf8_bin DEFAULT NULL,\\n\tPRIMARY KEY (`pk`),\\n\tUNIQUE `idx3` USING BTREE (`vc1`(20)),\\n\tKEY `idx1` USING HASH (`id1`),\\n\tKEY `idx2` USING HASH (`id2`),\\n\tFULLTEXT KEY `idx4` (`id3`),\\n\tGLOBAL INDEX gsi_id2(id2) COVERING (vc1, vc2) DBPARTITION BY HASH (id2) \\n\t) ENGINE = INNODB auto_increment = 2 avg_row_length = 100 DEFAULT CHARACTER SET utf8 COLLATE utf8_bin CHECKSUM = 0 COMMENT = \\\"abcd\\\" dbpartition BY HASH ( id1 );', 1624531778852, 1624531782210, 1, 3, 'RUNNING', 'ROLLBACK_RUNNING');";
            JdbcUtil.executeUpdateSuccess(metaConn, sql);
        } catch (Exception e) {

        }
        try {
            resourceManager
                .acquireResource(schemaName, 1344845353727295488L, Sets.newHashSet("a"), Sets.newHashSet("b", "c"));
            resourceManager
                .acquireResource(schemaName, 2L, Sets.newHashSet("a"), Sets.newHashSet("b", "c"));
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("ERR_PAUSED_DDL_JOB_EXISTS"));
            resourceManager
                .releaseResource(1344845353727295488L);
        }
    }

    @Test
    public void testRandomAcquireResource() {

        ExecutorCompletionService service = new ExecutorCompletionService(Executors.newFixedThreadPool(100));

        for (int i = 1; i <= 20; i++) {
            service.submit(getCallable(i));
        }

        try {
            for (int i = 1; i <= 20; i++) {
                service.take();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertTrue(
            rwLockManager.tryReadWriteLockBatch(schemaName, "DDL_" + 9999, new HashSet<>(), getAllResourcesSet())
        );

        rwLockManager.unlockReadWriteByOwner("DDL_" + 9999);
    }

    private Callable getCallable(int jobId) {
        Callable callable = () -> {
            try {
                for (int i = 0; i < 10; i++) {
                    Set<String> shared = getRandomResourceList(5, true);
                    Set<String> exclusive = getRandomResourceList(3, false);
                    resourceManager.acquireResource(schemaName, jobId, shared, exclusive);

                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        // interrupt it if necessary
                        Thread.currentThread().interrupt();
                    }

                    resourceManager.releaseResource(jobId);
                }
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return callable;
    }

    private Set<String> getRandomResourceList(int count, boolean lowerCase) {
        Set<String> resourceLsit = new HashSet<>();
        for (int i = 0; i < count; i++) {
            String r = getRandomResource();
            if (lowerCase) {
                resourceLsit.add(r.toLowerCase());
            } else {
                resourceLsit.add(r);
            }
        }
        return resourceLsit;
    }

    private String getRandomResource() {
        String resourceList = getAllResources();
        int r = RandomUtils.nextInt(0, resourceList.length());
        return String.valueOf(resourceList.charAt(r));
    }

    private Set<String> getAllResourcesSet() {
        Set<String> resourceSet = new HashSet<>();
        String resources = getAllResources();
        for (int i = 0; i < resources.length(); i++) {
            resourceSet.add(resources.charAt(i) + "");
        }
        return resourceSet;
    }

    private String getAllResources() {
        return "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    }

}
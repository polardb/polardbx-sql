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

import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.google.common.base.Strings;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class DdlTaskCompressTest {

    DdlJobManager ddlJobManager = new DdlJobManager();

    private static String schemaName = "mock_schema_name";

//    @BeforeClass
//    public static void beforeClass() {
//        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(polarDbXTestEnv.polardbxMetaDbAddr);
//        String metaDbIp = ipAndPort.getKey();
//        String metaDbPort = String.valueOf(ipAndPort.getValue());
//
//        String addr = metaDbIp + ":" + metaDbPort;
//        String dbName = polarDbXTestEnv.polardbxMetaDbName;
//        String props = "useUnicode=true&characterEncoding=utf-8&useSSL=false";
//        String usr = polarDbXTestEnv.polardbxMetaDbUser;
//        String pwd = polarDbXTestEnv.polardbxMetaDbPasswd;
//        MetaDbDataSource.initMetaDbDataSource(addr, dbName, props, usr, pwd);
//    }

    // don't delete me, I'm useful for local test
    @BeforeClass
    public static void beforeClass2() {
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr("127.0.0.1:3306");
        String metaDbIp = ipAndPort.getKey();
        String metaDbPort = String.valueOf(ipAndPort.getValue());

        String addr = metaDbIp + ":" + metaDbPort;
        String dbName = "polardbx_meta_db_polardbx";
        String props = "useUnicode=true&characterEncoding=utf-8&useSSL=false";
        String usr = "diamond";
        String pwd = "STA0HLmViwmos2woaSzweB9QM13NjZgrOtXTYx+ZzLw=";
        MetaDbDataSource.initMetaDbDataSource(addr, dbName, props, usr, pwd);
    }

    @Test
    public void testCompressDecompress() throws InterruptedException {

        final String payload = generateString(1024 * 1024 * 16);
        System.out.println("payload string length: " + payload.length());
        System.out.println("payload byte length: " + payload.getBytes().length);

        final DdlTask task = new TestPayloadTask("polardbx", payload);
        final Long jobId = task.getJobId();
        final Long taskId = task.getTaskId();

        SerializableClassMapper.register("TestPayloadTask", task.getClass());
        final DdlEngineTaskRecord record = TaskHelper.toDdlEngineTaskRecord(task);
        System.out.println("payload string length after compress: " + record.value.length());
        System.out.println("payload byte length after compress: " + record.value.getBytes().length);

        new DdlEngineAccessorDelegate<Void>() {
            @Override
            protected Void invoke() {

                engineTaskAccessor.insert(com.google.common.collect.Lists.newArrayList(record));
                DdlEngineTaskRecord recordFromDb = engineTaskAccessor.query(jobId, taskId);

                TestPayloadTask taskFromDb = (TestPayloadTask) TaskHelper.fromDdlEngineTaskRecord(recordFromDb);

                Assert.assertEquals(payload, taskFromDb.getPayload());
                return null;
            }
        }.execute();
    }

    private String generateString(int length) {
        StringBuilder sb = new StringBuilder();
        return Strings.padStart("", length, 'a');
    }

}
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
import com.alibaba.polardbx.gms.lease.LeaseManager;
import com.alibaba.polardbx.gms.lease.impl.LeaseManagerImpl;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.lease.LeaseRecord;
import org.apache.commons.lang3.RandomUtils;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Ignore
public class LeaseTest {

    private LeaseManager leaseManager = new LeaseManagerImpl();

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

    final String DEFAULT_SCHEMA = "polardbx";
    final String LEADER_KEY = "LEADER";

    @Test
    public void testLeaderElection() throws InterruptedException {
        Runnable elect = () -> {
            Optional<LeaseRecord> optionalLeaseRecord = leaseManager.acquire(DEFAULT_SCHEMA, LEADER_KEY, 3_000);
            if(optionalLeaseRecord.isPresent()){
                System.out.println("acquire leader success, id=" + optionalLeaseRecord.get().getId());
            }else {
                System.out.println("acquire leader failed");
            }

            long timeRemaining = leaseManager.timeRemaining(LEADER_KEY);
            System.out.println("leader time remaining: " + timeRemaining);


            System.out.println("leader time remaining2: " + (optionalLeaseRecord.get().getExpireAt() - System.currentTimeMillis()));
        };

        Runnable extend = () -> {
            Optional<LeaseRecord> optionalLeaseRecordExtend = leaseManager.extend(LEADER_KEY);
            if(optionalLeaseRecordExtend.isPresent()){
                System.out.println("extend leader success, id=" + optionalLeaseRecordExtend.get().getId());
            }else {
                System.out.println("extend leader failed");
            }

            long timeRemaining = leaseManager.timeRemaining(LEADER_KEY);
            System.out.println("leader time remaining2: " + timeRemaining);

            System.out.println("leader time remaining2: " + (optionalLeaseRecordExtend.get().getExpireAt() - System.currentTimeMillis()));
        };

        Optional<LeaseRecord> optionalLeaseRecord = leaseManager.acquire(DEFAULT_SCHEMA, LEADER_KEY, 3_000);
        if(optionalLeaseRecord.isPresent()){
            System.out.println("acquire leader success, id=" + optionalLeaseRecord.get().getId());
        }else {
            System.out.println("acquire leader failed");
        }
        while (true){
            long timeRemaining = leaseManager.timeRemaining(LEADER_KEY);
            System.out.println("leader time remaining: " + timeRemaining);

            System.out.println("valid: " + optionalLeaseRecord.get().valid());

            TimeUnit.SECONDS.sleep(1);
        }

//        long nano1 = System.nanoTime();
//        System.out.println("nano1: " + nano1);
//
//        TimeUnit.SECONDS.sleep(1);
//
//        long nano2 = System.nanoTime();
//        System.out.println("nano2: " + nano2);
//
//        System.out.println("diff: " + (nano2 - nano1));
    }

    private String getRandomResource() {
        String resourceList = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        int r = RandomUtils.nextInt(0, resourceList.length());
        return String.valueOf(resourceList.charAt(r));
    }

}
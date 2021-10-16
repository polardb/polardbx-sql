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

package com.alibaba.polardbx.executor.mdl;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;

@Ignore
public class MdlTest {

//    //todo guxu implement me
//    @Test
//    @Ignore
//    public void testGet() {
//        String schemaName = "foobar";
//        String tableNameDigest = "t1:v1";
//
//        // Lock and unlock MDL on primary table to clear cross status transaction.
//        final MdlContext client = createContext("300");
//        final MdlContext system = createContext(schemaName);
//        final MdlContext client2 = createContext("301");
//
//        System.out.println("try acquire S lock, 300");
//
//        final MdlTicket ticket = client.acquireLock(new MdlRequest(
//            1L,
//            MdlKey.getTableKeyWithLowerTableName(schemaName, tableNameDigest),
//            MdlType.MDL_SHARED_WRITE,
//            MdlDuration.MDL_TRANSACTION));
//        System.out.println("S lock acquired, 300");
//
//        Thread t2 = new Thread(() -> {
//            System.out.println("try acquire X lock");
//            final MdlTicket ticket2 = system.acquireLock(new MdlRequest(
//                1L + Long.MAX_VALUE,
//                MdlKey.getTableKeyWithLowerTableName(schemaName, tableNameDigest),
//                MdlType.MDL_EXCLUSIVE,
//                MdlDuration.MDL_TRANSACTION));
//            System.out.println("X lock acquired");
//        });
//        t2.start();
//
//        Thread.sleep(100);
//
//        Thread t3 = new Thread(() -> {
//            System.out.println("try acquire S lock, 301");
//            final MdlTicket ticket2 = client2.acquireLock(new MdlRequest(
//                2L,
//                MdlKey.getTableKeyWithLowerTableName(schemaName, tableNameDigest),
//                MdlType.MDL_SHARED_WRITE,
//                MdlDuration.MDL_TRANSACTION));
//            System.out.println("S lock acquired, 301");
//        });
//        t3.start();
//
//        MdlKey mdlKey = MdlKey.getTableKeyWithLowerTableName(schemaName, tableNameDigest);
//        System.out.println("client wait for: " + Arrays.toString(client.getWaitFor(mdlKey).toArray()));
//        System.out.println("system wait for: " + Arrays.toString(system.getWaitFor(mdlKey).toArray()));
//        System.out.println("client2 wait for: " + Arrays.toString(client2.getWaitFor(mdlKey).toArray()));
//
//        Thread.sleep(3000);
//
//        client.releaseAllTransactionalLocks();
//        client2.releaseAllTransactionalLocks();
//        system.releaseAllTransactionalLocks();
//    }
//
//    private MdlContext createContext(String contextId) {
//        final MdlContext context = MdlManager.addContext(contextId, true);
//        final MdlContext system = createContext(schemaName);
//        final MdlTicket ticket2 = system.acquireLock(new MdlRequest(
//            1L + Long.MAX_VALUE,
//            MdlKey.getTableKeyWithLowerTableName(schemaName, tableNameDigest),
//            MdlType.MDL_EXCLUSIVE,
//            MdlDuration.MDL_TRANSACTION));
//
//        client.releaseAllTransactionalLocks();
//        system.releaseAllTransactionalLocks();
//    }
//
//    private MdlContext createContext(String contextId) {
//        final MdlContext context = MdlManager.addContext(contextId);
//        return context;
//    }

}

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
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Ignore
public class RWLockTest {

    private PersistentReadWriteLock manager = PersistentReadWriteLock.create();

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
//     @BeforeClass
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

    /**
     * 测试
     * 读锁可重入
     * 读锁可释放
     */
    @Test
    public void testReadLock() {
        manager.readLock(schemaName, "1", "foo");
        manager.readLock(schemaName, "1", "foo"); //读锁可重入
        Assert.assertTrue(manager.hasReadLock("1", "foo"));
        manager.readLock(schemaName, "2", "foo"); //读锁可重入
        Assert.assertTrue(manager.hasReadLock("2", "foo"));
        manager.unlockRead("1", "foo"); //读锁可释放
        manager.unlockRead("2", "foo"); //读锁可释放
        Assert.assertTrue(manager.hasReadLock("1", "foo") == false);
        Assert.assertTrue(manager.hasReadLock("2", "foo") == false);

        manager.writeLock(schemaName, "2", "foo"); //读锁可释放
        Assert.assertTrue(manager.hasWriteLock("2", "foo"));
        manager.unlockWrite("2", "foo");
        Assert.assertTrue(manager.hasWriteLock("2", "foo") == false);
    }

    /**
     * 测试
     * 写锁可重入
     * 写锁可释放
     */
    @Test
    public void testWriteLock() {
        manager.writeLock(schemaName, "1", "foo");
        manager.writeLock(schemaName, "1", "foo"); //写锁可重入
        manager.writeLock(schemaName, "1", "foo"); //写锁可重入
        manager.unlockWrite("1", "foo"); //写锁可释放

        manager.readLock(schemaName, "2", "foo"); //写锁可释放
        manager.unlockRead("2", "foo");
    }

    /**
     * 测试
     * 读写锁互斥
     */
    @Test
    public void testReadBlockWrite() {
        manager.readLock(schemaName, "1", "foo");
        manager.readLock(schemaName, "1", "foo");
        Assert.assertTrue(manager.hasReadLock("1", "foo"));
        manager.readLock(schemaName, "2", "foo");
        manager.readLock(schemaName, "2", "foo");
        Assert.assertTrue(manager.hasReadLock("2", "foo"));
        manager.readLock(schemaName, "3", "foo");
        manager.readLock(schemaName, "3", "foo");
        Assert.assertTrue(manager.hasReadLock("3", "foo"));

        manager.unlockRead("1", "foo");
        manager.unlockRead("2", "foo");

        //读锁阻塞写锁
        Assert.assertFalse(manager.tryWriteLock(schemaName, "4", "foo"));
        Assert.assertTrue(manager.hasWriteLock("4", "foo") == false);
        //如果是同一个owner，读锁可隐式升级为写锁
        Assert.assertTrue(manager.tryWriteLock(schemaName, "3", "foo"));
        Assert.assertTrue(manager.hasWriteLock("3", "foo"));
        Assert.assertTrue(manager.hasReadLock("3", "foo") == false);
        //升级后的写锁，依然可重入
        Assert.assertTrue(manager.tryWriteLock(schemaName, "3", "foo"));
        Assert.assertTrue(manager.hasWriteLock("3", "foo"));
        Assert.assertTrue(manager.tryReadLock(schemaName, "3", "foo"));
        Assert.assertTrue(manager.hasReadLock("3", "foo") == false);
        Assert.assertTrue(manager.hasWriteLock("3", "foo"));

        manager.unlockRead("3", "foo");
        Assert.assertTrue(manager.hasReadLock("3", "foo") == false);
        Assert.assertTrue(manager.hasWriteLock("3", "foo"));
        manager.unlockWrite("3", "foo");
        Assert.assertTrue(manager.hasWriteLock("3", "foo") == false);
    }

    /**
     * 测试
     * 写读锁互斥
     */
    @Test
    public void testWriteBlockRead() {
        manager.writeLock(schemaName, "1", "foo");

        Assert.assertFalse(manager.tryReadLock(schemaName, "3", "foo"));

        manager.unlockWrite("1", "foo");

        Assert.assertTrue(manager.tryReadLock(schemaName, "3", "foo"));

        manager.unlockRead("3", "foo");
    }

    /**
     * 测试
     * 写写锁互斥
     */
    @Test
    public void testWriteBlockWrite() {
        manager.writeLock(schemaName, "1", "foo");

        Assert.assertFalse(manager.tryWriteLock(schemaName, "3", "foo"));

        manager.unlockWrite("1", "foo");

        Assert.assertTrue(manager.tryWriteLock(schemaName, "3", "foo"));

        manager.writeLock(schemaName, "3", "foo");
        manager.unlockWrite("3", "foo");
    }

    /**
     * 测试
     * 获取写锁后可获取读锁
     */
    @Test
    public void testReadAfterWrite() {
        Assert.assertTrue(manager.tryWriteLock(schemaName, "3", "foo"));
        Assert.assertTrue(manager.tryReadLock(schemaName, "3", "foo"));
        Assert.assertTrue(manager.tryReadLock(schemaName, "3", "foo"));
        //依然被死锁阻塞
        Assert.assertFalse(manager.tryReadLock(schemaName, "2", "foo"));

        Assert.assertTrue(manager.unlockRead("3", "foo") == 0);
        Assert.assertTrue(manager.unlockWrite("3", "foo") == 1);
    }

    /**
     * 测试
     * 隐式锁升级
     * 不支持隐式锁降级
     */
    @Test
    public void testWriteAfterRead() {
        Assert.assertTrue(manager.tryReadLock(schemaName, "3", "foo"));
        Assert.assertTrue(manager.tryWriteLock(schemaName, "3", "foo"));

        Assert.assertTrue(manager.unlockRead("3", "foo") == 0);
        Assert.assertTrue(manager.unlockWrite("3", "foo") == 1);
    }

    /**
     * 测试批量获取锁
     */
    @Test
    public void test4() {
        Assert.assertTrue(manager.tryReadLock(schemaName, "1", "foo"));

        Assert.assertTrue(manager.tryWriteLockBatch(schemaName, "1", Sets.newHashSet("foo", "bar", "goo")));

        Assert.assertTrue(manager.unlockRead("1", "foo") == 0);
        Assert.assertTrue(manager.unlockWrite("1", "foo") == 1);

        Assert.assertTrue(manager.tryWriteLockBatch(schemaName, "1", Sets.newHashSet("foo", "bar", "goo")));

        Assert.assertTrue(manager.unlockWriteBatch("1", Sets.newHashSet("foo", "bar", "goo")) == 3);
    }

    /**
     * 测试同时批量获取读锁和写锁
     */
    @Test
    public void test5() {
        Assert.assertTrue(manager.tryReadWriteLockBatch(schemaName,
            "1",
            Sets.newHashSet(),
            Sets.newHashSet()));
        Assert.assertTrue(manager.tryReadWriteLockBatch(schemaName,
            "1",
            Sets.newHashSet("foo", "bar", "goo"),
            Sets.newHashSet("foo", "bar", "goo")));

        Assert.assertTrue(manager.hasWriteLock("1", "foo"));
        Assert.assertTrue(manager.hasWriteLock("1", "bar"));
        Assert.assertTrue(manager.hasWriteLock("1", "goo"));
        Assert.assertFalse(manager.hasReadLock("1", "foo"));
        Assert.assertFalse(manager.hasReadLock("1", "bar"));
        Assert.assertFalse(manager.hasReadLock("1", "goo"));

        Assert.assertTrue(manager.tryReadWriteLockBatch(schemaName,
            "1",
            Sets.newHashSet("foo", "bar", "goo"),
            Sets.newHashSet("foo", "bar", "goo")));
        Assert.assertFalse(manager.tryReadWriteLockBatch(schemaName,
            "2",
            Sets.newHashSet("foo"),
            Sets.newHashSet()));
        Assert.assertEquals(manager.unlockReadWriteByOwner("1"), 3);

        Assert.assertFalse(manager.hasWriteLock("1", "foo"));
        Assert.assertFalse(manager.hasWriteLock("1", "bar"));
        Assert.assertFalse(manager.hasWriteLock("1", "goo"));
    }

    @Test
    public void testDownGrade() {
        final String owner = "1";
        final String lock1 = "LOCK1";
        final String lock2 = "LOCK2";
        Assert.assertTrue(manager.tryReadWriteLockBatch(schemaName,
            owner,
            Sets.newHashSet(),
            Sets.newHashSet(lock1)));

        Assert.assertTrue(manager.hasWriteLock(owner, lock1));

        Assert.assertTrue(!manager.downGradeWriteLock(MetaDbDataSource.getInstance().getConnection(), "2", lock1));

        Assert.assertTrue(manager.downGradeWriteLock(MetaDbDataSource.getInstance().getConnection(), owner, lock1));

        Assert.assertTrue(!manager.downGradeWriteLock(MetaDbDataSource.getInstance().getConnection(), owner, lock2));

        Assert.assertTrue(!manager.hasWriteLock(owner, lock1));
        Assert.assertTrue(manager.hasReadLock(owner, lock1));

        Assert.assertTrue(!manager.hasWriteLock(owner, lock2));
        Assert.assertTrue(!manager.hasReadLock(owner, lock2));

        Assert.assertTrue(manager.unlockRead(owner, lock1) == 1);
    }

    @Test
    public void testLargeBatch() {

        Set<String> readLocks = new HashSet<>();
        Set<String> writeLocks = new HashSet<>();

        for (int i = 0; i < 20000; i++) {
            readLocks.add(String.valueOf(i));
        }

        for (int i = 20000; i < 40000; i++) {
            writeLocks.add(String.valueOf(i));
        }

        Assert.assertTrue(
            manager.tryReadWriteLockBatch(
                "polardbx",
                "1",
                readLocks,
                writeLocks
            )
        );
    }

    @Test
    @Ignore
    public void randomTest() {
        Callable t1 = () -> {
            try {
                while (true) {
                    String resource = getRandomResource();
                    manager.tryReadLock(schemaName, "1", resource);
                    Set<String> r = Sets.newHashSet(getRandomResource());
                    manager.writeLockBatch(schemaName, "1", r);

                    manager.unlockRead("1", resource);
                    manager.unlockWriteBatch("1", r);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Callable t2 = () -> {
            while (true) {
                String resource = getRandomResource();
                manager.tryWriteLock(schemaName, "2", resource);
                manager.unlockWrite("2", resource);
            }
        };
        Callable t3 = () -> {
            while (true) {
                String resource = getRandomResource();
                manager.readLock(schemaName, "3", resource);
                Set<String> r = Sets.newHashSet(getRandomResource());
                manager.writeLockBatch(schemaName, "3", r);

                manager.unlockRead("3", resource);
                manager.unlockWriteBatch("3", r);
            }
        };
        Callable t4 = () -> {
            while (true) {
                String resource = getRandomResource();
                manager.writeLock(schemaName, "4", resource);
                manager.unlockWrite("4", resource);
            }
        };
        Callable t5 = () -> {
            while (true) {
                String resource = getRandomResource();
                manager.tryReadLock(schemaName, "5", resource);
                manager.unlockRead("5", resource);
            }
        };
        Callable t6 = () -> {
            while (true) {
                Set<String> resource = Sets.newHashSet(getRandomResource(), getRandomResource(), getRandomResource());
                manager.writeLockBatch(schemaName, "6", resource);
                manager.unlockWriteBatch("6", resource);
            }
        };

        ExecutorCompletionService service = new ExecutorCompletionService(Executors.newFixedThreadPool(10));
        Future f1 = service.submit(t1);
//        Future f2 = service.submit(t2);
        Future f3 = service.submit(t3);
        Future f4 = service.submit(t4);
//        Future f5 = service.submit(t5);
        Future f6 = service.submit(t6);

        try {
            TimeUnit.SECONDS.sleep(30);
            service.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            f1.get();
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
//            f2.get();
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
            f3.get();
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
            f4.get();
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
//            f5.get();
        } catch (Exception e) {
            System.out.println(e);
        }
        try {
            f6.get();
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    private String getRandomResource() {
        String resourceList = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        int r = RandomUtils.nextInt(0, resourceList.length());
        return String.valueOf(resourceList.charAt(r));
    }

}
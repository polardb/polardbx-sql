package com.alibaba.polardbx.executor.mdl;

import com.alibaba.polardbx.executor.mdl.manager.MdlManagerStamped;
import net.bytebuddy.implementation.bytecode.Throw;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.executor.mdl.MdlManager.SCHEMA_MDL_MANAGERS;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class MdlManagerStampedMoreTest {

    private final String schema = "mdl_test_schema";
    private final AtomicLong connIdCounter = new AtomicLong(0);
    private final AtomicLong trxIdCounter = new AtomicLong(0);
    private final String tableName = "mdl_table";
    private final AtomicInteger tableVersion = new AtomicInteger(0);

    private final AtomicInteger invalideVersionMax = new AtomicInteger(-1);

    private MdlManager mdlManager;

    private int schemaChangeCnt = 4;

    @Before
    public void setUp() throws Exception {
        mdlManager = new MdlManagerStamped(schema, 1);
        SCHEMA_MDL_MANAGERS.put(schema, mdlManager);
    }

    @Test
    public void testSchemaChange() throws ExecutionException, InterruptedException {
        /**
         * ddl update table version and evacuate old transaction
         * */
        final ExecutorService ddlPool = Executors.newFixedThreadPool(1);

        Long ddlConnId = connIdCounter.addAndGet(1);
        Long ddlTrxId = trxIdCounter.addAndGet(1);

        List<MdlTicket> ddlTickets = new ArrayList<>();
        Future ddlFuture = ddlPool.submit(() -> {
            for (int i = 0; i < schemaChangeCnt; i++) {
                // update version
                long version = this.tableVersion.get();

                toNewVersion();

                //evacuate old version
                final MdlContext context = mdlManager.addContext(ddlConnId);
                final MdlTicket ticket = context.acquireLock(writeRequest(ddlTrxId, digest(tableName, version)));
                ddlTickets.add(ticket);

                invalideVersionMax.addAndGet(1);

                // do ddl task
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        /**
         * dml
         * */
        final ExecutorService dmlPool = Executors.newFixedThreadPool(10);
        AtomicBoolean dmlAcquireOldVersionSucceed = new AtomicBoolean(false);

        IntStream.range(0, 10).forEach(index -> {
            final Future<?> future = dmlPool.submit(() -> {
                while (true) {
                    final Random r = new Random(LocalDateTime.now().getNano());
                    try {
                        TimeUnit.MILLISECONDS.sleep(r.nextInt(200) + 1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    //begin
                    int version = this.tableVersion.get();
                    Long trxConnId = connIdCounter.addAndGet(1);
                    Long trxId = trxIdCounter.addAndGet(1);

                    //execute dml
                    try {
                        TimeUnit.MILLISECONDS.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }

                    final MdlContext context = mdlManager.addContext(trxConnId);
                    final MdlTicket ticket = context.acquireLock(readRequest(trxId, digest(tableName, version)));
                    if (!versionCheck(trxId, (long) version)) {
                        dmlAcquireOldVersionSucceed.set(true);
                    }
                    //commit
                    context.releaseLock(trxId, ticket);
                    if (!versionCheck(trxId, (long) version)) {
                        dmlAcquireOldVersionSucceed.set(true);
                    }

                    // close connection
                    final MdlContext mdlContext = mdlManager.removeContext(context);

                    if (Thread.interrupted()) {
                        break;
                    }
                }
            });
        });

        try {
            ddlFuture.get();
            dmlPool.shutdownNow();
        } catch (Throwable ignore) {
        }

        mdlManager.getWaitFor(ddlTickets.get(0).getLock().getKey());
        ddlTickets.forEach(ticket -> {
            mdlManager.releaseLock(ticket);
        });

        Assert.assertTrue(!dmlAcquireOldVersionSucceed.get());
    }

    private boolean versionCheck(Long trxId, Long version) {
        int maxInvalidateVersion = this.invalideVersionMax.get();
        return version > maxInvalidateVersion;
    }

    private void toNewVersion() {
        tableVersion.addAndGet(1);
    }

    private String digest(String tableName, long version) {
        return tableName + "#version:" + version;
    }

    private MdlRequest readRequest(Long trxId, String tableName) {

        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(schema, tableName),
            MdlType.MDL_SHARED_WRITE,
            MdlDuration.MDL_TRANSACTION);
    }

    private MdlRequest writeRequest(Long trxId, String tableName) {

        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(schema, tableName),
            MdlType.MDL_EXCLUSIVE,
            MdlDuration.MDL_TRANSACTION);
    }

}

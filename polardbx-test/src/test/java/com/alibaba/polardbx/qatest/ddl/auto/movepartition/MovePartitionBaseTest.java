package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.sharding.repartition.RepartitionBaseTest;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.IntStream;

public class MovePartitionBaseTest extends DDLBaseNewDBTestCase {

    public MovePartitionBaseTest(String currentDb) {
        this.currentDb = currentDb;
    }

    public String getCurrentDb() {
        return currentDb;
    }

    protected String currentDb;

    protected static class DDLRequest {
        public void executeDdl() throws Exception {
            // do nothing
        }
    }

    protected void doInsertWhileDDL(String insertSqlTemplate, int threadCount, DDLRequest ddlRequest) throws Exception {
        final ExecutorService dmlPool = new ThreadPoolExecutor(
            threadCount,
            threadCount,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        final AtomicBoolean allStop = new AtomicBoolean(false);
        Function<Connection, Integer> call = connection -> {
            // List<Pair< sql, error_message >>
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            try {
                return gsiExecuteUpdate(connection, mysqlConnection, insertSqlTemplate, failedList, false, false);
            } catch (SQLSyntaxErrorException e) {
                if (StringUtils.contains(e.toString(), "ERR_TABLE_NOT_EXIST")) {
                    return 0;
                } else {
                    throw GeneralUtil.nestedException(e);
                }
            } catch (AssertionError e) {
                if (StringUtils.contains(e.toString(), "Communications link failure")) {
                    return 0;
                } else {
                    throw GeneralUtil.nestedException(e);
                }
            }
        };

        List<Future> inserts = new ArrayList<>();

        IntStream.range(0, threadCount).forEach(
            i -> inserts.add(dmlPool.submit(new RepartitionBaseTest.InsertRunner(allStop, call, getCurrentDb())))
        );

        try {
            TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        ddlRequest.executeDdl();

        allStop.set(true);
        dmlPool.shutdown();

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

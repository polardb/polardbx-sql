package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Getter;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

@Getter
public class MovePartitionDmlBaseTest extends DDLBaseNewDBTestCase {

    protected static final String SELECT_IGNORE_TMPL =
        "/*+TDDL: cmd_extra(ENABLE_MPP = false)*/select id,c1,c2 from {0} ignore index({1}) order by id";

    protected static final String SELECT_FORCE_TMPL =
        "/*+TDDL: cmd_extra(ENABLE_MPP = false)*/select id,c1,c2 from {0} force index({1}) order by id";

    protected static final String SLOW_HINT =
        "/*+TDDL: cmd_extra(GSI_DEBUG=\"slow\")*/";

    protected static final String SHOW_DS = "show ds where db='%s'";
    protected static final String MOVE_PARTITION_COMMAND = "alter table %s move partitions %s to '%s'";

    protected static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    protected static final String SELECT_FROM_TABLE_DETAIL_GSI =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and index_name='%s' and partition_name='%s'";

    public MovePartitionDmlBaseTest(String currentDb) {
        this.currentDb = currentDb;
    }

    protected String currentDb;

    protected static boolean notIgnoredErrors(Exception e) {
        return !e.getMessage().contains("Deadlock found when trying to get lock") &&
            !e.getMessage().contains("Incorrect string value") &&
            !e.getMessage().contains("Lock wait timeout exceeded") &&
            !e.getMessage().contains("Duplicate entry");
    }

    protected static final Consumer<Exception> throwException = (e) -> {
        if (!e.getMessage().contains("Communications link failure") &&
            !e.getMessage().contains("No operations allowed")) {
            throw GeneralUtil.nestedException(e);
        }
    };

    protected class InsertRunner implements Runnable {

        private final AtomicBoolean stop;
        private final Function<Connection, Integer> call;
        private final Consumer<Exception> errHandler;
        private final int maxSeconds;

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call) {
            this(stop, call, throwException, 10); // Default insert for 10s.
        }

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call, Consumer<Exception> errHandler) {
            this(stop, call, errHandler, 10);
        }

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call, Consumer<Exception> errHandler,
                            int maxSeconds) {
            this.stop = stop;
            this.call = call;
            this.errHandler = errHandler;
            this.maxSeconds = maxSeconds;
        }

        @Override
        public void run() {
            final long startTime = System.currentTimeMillis();
            int count = 0;
            do {
                try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
                    JdbcUtil.useDb(conn, getCurrentDb());
                    count += call.apply(conn);
                } catch (Exception e) {
                    errHandler.accept(e);
                }

                if (System.currentTimeMillis() - startTime > maxSeconds * 1000) {
                    break; // 10s timeout, because we check after create GSI(which makes create GSI far more slower.).
                }
            } while (!stop.get());

            System.out.println(Thread.currentThread().getName() + " quit after " + count + " records inserted");
        }
    }

    protected void doReCreateDatabase() {
        doClearDatabase();
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "create database " + getCurrentDb() + " partition_mode = 'auto'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + getCurrentDb();
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
    }

    protected void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql = "drop database if exists " + getCurrentDb();
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}

package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class ShareReadViewInRcTest extends CrudBasedLockTestCase {
    @Test
    public void test() throws InterruptedException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_SHARE_READVIEW_IN_RC=true");

        final String TABLE_NAME = "ShareReadViewInRcTest_tb";
        final String CREATE_TABLE = "create table if not exists " + TABLE_NAME
            + " (id int primary key) dbpartition by hash(id)";
        final String DROP_TABLE = "drop table if exists " + TABLE_NAME;
        JdbcUtil.executeUpdateSuccess(tddlConnection, DROP_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE);
        final String INSERT_DATA = "insert into " + TABLE_NAME + " values(0), (1), (2), (3)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, INSERT_DATA);
        final String SELECT_DATA = "select * from " + TABLE_NAME;

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set TRANSACTION_POLICY='XA'");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set TX_ISOLATION='READ-COMMITTED'");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set TRANSACTION_ISOLATION='READ-COMMITTED'");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ALLOW_GROUP_PARALLELISM_WITHOUT_SHARE_READVIEW=true");

        JdbcUtil.executeQuerySuccess(tddlConnection, SELECT_DATA);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global ENABLE_SHARE_READVIEW_IN_RC=false");
        Thread.sleep(1000);
        JdbcUtil.executeQuerySuccess(tddlConnection, SELECT_DATA);

    }
}

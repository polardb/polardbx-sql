package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */

@NotThreadSafe
public class AlterDatabaseTest extends DDLBaseNewDBTestCase {
    private static final String drdsDb = "testReadOnlyDrdsDb";
    private static final String autoDb = "testReadOnlyAutoDb";

    private static final String createDrdsDb = "create database {0} mode=drds";
    private static final String createAutoDb = "create database {0} mode=auto";
    private static final String dropDbSql = "drop database if exists {0} ";

    private static final String createTableSql = "create table tb1 (id int) broadcast";
    private static final String dql = "select * from tb1 limit 1";
    private static final String dmlDelete = "delete from tb1 where id = 0";
    private static final String dmlInsert = "insert into tb1 select * from tb1";
    private static final String dmlUpdate = "update tb1 set id = 0 where id = 0";
    private static final String alterToReadOnly = "alter database {0} set read_only=true";
    private static final String alterToReadWrite = "alter database {0} set read_only=false";

    @Before
    public void prepareDb() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, drdsDb));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, autoDb));

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createDrdsDb, drdsDb));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createAutoDb, autoDb));

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("use {0}", drdsDb));
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("use {0}", autoDb));
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql);
    }

    @After
    public void dropDb() {
        useDb(tddlConnection, tddlDatabase1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, drdsDb));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, autoDb));
    }

    @Test
    public void testSetReadOnly() {
        //test drds db read_only
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(alterToReadOnly, drdsDb));
        useDb(tddlConnection, drdsDb);
        JdbcUtil.executeQuerySuccess(tddlConnection, dql);
        JdbcUtil.executeUpdateFailed(tddlConnection, dmlDelete, "read only");
        JdbcUtil.executeUpdateFailed(tddlConnection, dmlInsert, "read only");
        JdbcUtil.executeUpdateFailed(tddlConnection, dmlUpdate, "read only");
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(alterToReadWrite, drdsDb));
        JdbcUtil.executeQuerySuccess(tddlConnection, dql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlDelete);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlInsert);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlUpdate);

        //test auto db read_only
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(alterToReadOnly, autoDb));
        useDb(tddlConnection, autoDb);
        JdbcUtil.executeQuerySuccess(tddlConnection, dql);
        JdbcUtil.executeUpdateFailed(tddlConnection, dmlDelete, "read only");
        JdbcUtil.executeUpdateFailed(tddlConnection, dmlInsert, "read only");
        JdbcUtil.executeUpdateFailed(tddlConnection, dmlUpdate, "read only");
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(alterToReadWrite, autoDb));
        JdbcUtil.executeQuerySuccess(tddlConnection, dql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlDelete);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlInsert);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlUpdate);

    }
}

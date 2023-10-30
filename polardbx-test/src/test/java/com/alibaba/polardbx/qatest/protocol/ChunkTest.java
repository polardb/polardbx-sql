package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;

import static com.alibaba.polardbx.qatest.BaseSequenceTestCase.quoteSpecialName;

public class ChunkTest extends ReadBaseTestCase {

    private final String TABLE_NAME = "chunk_test";

    private static final String TABLE_TEMPLATE = "create table {0} (\n"
        + "    pk bigint not null auto_increment,\n"
        + "    x int default null,\n"
        + "    primary key(pk)\n"
        + ") dbpartition by hash(pk)";

    @Before
    public void initTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            MessageFormat
                .format(TABLE_TEMPLATE,
                    quoteSpecialName(TABLE_NAME)));
    }

    @After
    public void cleanup() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
    }

    private void initData() {
        for (int i = 0; i < 1000; ++i) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "insert into " + quoteSpecialName(TABLE_NAME)
                    + " (x) values (null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null),"
                    + "(null),(null),(null),(null),(null),(null),(null),(null),(null),(null);");
        }
    }

    @Test
    public void testChunkNullBitmapOOB() {
        initData();
        JdbcUtil.executeQuerySuccess(tddlConnection,
            "select x from " + quoteSpecialName(TABLE_NAME) + " limit 1000000");
    }
}

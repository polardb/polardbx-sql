package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.util.List;

public class RemovePartitioningTest extends DDLBaseNewDBTestCase {

    @Test
    public void testRemovePartitioning() {
        String sql1 = "drop table if exists remove_partitioning_t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 =
            "create table remove_partitioning_t1(a int primary key, b varchar(100), index idxb(b)) partition by key(b)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "trace alter table remove_partitioning_t1 remove partitioning";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        List<List<String>> traceResult = getTrace(tddlConnection);
        for (List<String> row : traceResult) {
            for (String col : row) {
                if (col != null && col.toLowerCase().contains("CdcGsiDdlMarkTask".toLowerCase())) {
                    Assert.fail("CdcGsiDdlMarkTask should not be executed");
                }
            }
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}

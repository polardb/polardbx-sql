package com.alibaba.polardbx.qatest.ddl.sharding.omc;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class OMCMultiWriteTest extends DDLBaseNewDBTestCase {
    final String hint1 =
        " /*+TDDL:CMD_EXTRA(FORCE_USING_OMC=true,GSI_FINAL_STATUS_DEBUG=DELETE_ONLY,REPARTITION_SKIP_CUTOVER=true,REPARTITION_SKIP_CLEANUP=true) */";

    final String hint2 =
        " /*+TDDL:CMD_EXTRA(FORCE_USING_OMC=true,GSI_FINAL_STATUS_DEBUG=WRITE_ONLY,REPARTITION_SKIP_CUTOVER=true,REPARTITION_SKIP_CLEANUP=true) */";

    protected final String dmlHintStr =
        " /*+TDDL:cmd_extra(PLAN_CACHE=false,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_USE_RETURNING=FALSE)*/ ";

    @Override
    public boolean usingNewPartDb() {
        return false;
    }

    @Test
    public void testStayAtDeleteOnly() {
        String tableName = "omc_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` modify column b int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 1);

        //因为是delete only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //预期：select + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + delete + delete + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);
        assertTraceContains(trace, "INSERT", 1);

        //预期：select + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + delete + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));
        assertTraceContains(trace, "DELETE", 1);
    }

    @Test
    public void testStayAtWriteOnly() {
        String tableName = "omc_test_tbl2" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` modify b int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        // select + update + update
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //因为是 write only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //预期：select + insert + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + delete + insert + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);
        assertTraceContains(trace, "INSERT", 2);

        //预期：select + insert + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + insert + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));
        assertTraceContains(trace, "DELETE", 1);
    }

    @Test
    public void testStayAtDeleteOnlyWithGsi() {
        String tableName = "omc_test_tbl3" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add global index omc_test_gsi3(a, b, c) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` modify b int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //因为是delete only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);

        //预期：select + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + delete + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);
        assertTraceContains(trace, "INSERT", 2);

        //预期：select + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));
        assertTraceContains(trace, "DELETE", 2);
    }

    @Test
    public void testStayAtWriteOnlyWithGsi() {
        String tableName = "omc_test_tbl4" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add global index omc_test_gsi4(a, b, c) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` modify b int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));

        // select + update + update
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));

        //因为是 write only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);

        //预期：select + insert + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));

        //预期：select + delete + delete + insert + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);
        assertTraceContains(trace, "INSERT", 4);

        //预期：select + insert + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));

        //预期：select + delete + insert + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(7));
        assertTraceContains(trace, "DELETE", 2);
    }

    @Test
    public void testStayAtDeleteOnlyWithChangeColumn() {
        String tableName = "omc_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` change b d int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 1);

        //因为是delete only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //预期：select + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + delete + delete + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);
        assertTraceContains(trace, "INSERT", 1);

        //预期：select + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //预期：select + delete + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));
        assertTraceContains(trace, "DELETE", 1);
    }

    @Test
    public void testStayAtWriteOnlyWithChangeColumn() {
        String tableName = "omc_test_tbl2" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` change column b d int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        // select + update + update
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //因为是 write only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //预期：select + insert + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + delete + insert + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);
        assertTraceContains(trace, "INSERT", 2);

        //预期：select + insert + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + insert + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));
        assertTraceContains(trace, "DELETE", 1);
    }

    @Test
    public void testStayAtDeleteOnlyWithGsiWithChangeColumn() {
        String tableName = "omc_test_tbl3" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add global index omc_test_gsi5(a, b, c) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` change column b d int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //因为是delete only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);

        //预期：select + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + delete + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);
        assertTraceContains(trace, "INSERT", 2);

        //预期：select + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(3));

        //预期：select + delete + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));
        assertTraceContains(trace, "DELETE", 2);
    }

    @Test
    public void testStayAtWriteOnlyWithGsiWithChangeColumn() {
        String tableName = "omc_test_tbl4" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add global index omc_test_gsi6(a, b, c) dbpartition by hash(`a`)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` change column b d int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));

        // select + update + update
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));

        //因为是 write only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);

        //预期：select + insert + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));

        //预期：select + delete + delete + insert + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);
        assertTraceContains(trace, "INSERT", 4);

        //预期：select + insert + insert
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));

        //预期：select + delete + insert + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(7));
        assertTraceContains(trace, "DELETE", 2);
    }

    protected void assertTraceContains(List<List<String>> trace, String targetStr, int count) {
        int c = 0;
        for (List<String> item : trace) {
            for (String s : item) {
                if (StringUtils.containsIgnoreCase(s, targetStr)) {
                    c++;
                }
            }
        }
        //make sure now() is pushed down, instead of logical execution
        org.junit.Assert.assertEquals(count, c);
    }
}
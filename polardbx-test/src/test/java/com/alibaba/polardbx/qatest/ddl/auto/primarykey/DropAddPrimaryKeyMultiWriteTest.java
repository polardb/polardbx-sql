package com.alibaba.polardbx.qatest.ddl.auto.primarykey;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;

public class DropAddPrimaryKeyMultiWriteTest extends DDLBaseNewDBTestCase {
    final String hint1 =
        " /*+TDDL:CMD_EXTRA(GSI_FINAL_STATUS_DEBUG=DELETE_ONLY,REPARTITION_SKIP_CUTOVER=true,REPARTITION_SKIP_CLEANUP=true) */";

    final String hint2 =
        " /*+TDDL:CMD_EXTRA(GSI_FINAL_STATUS_DEBUG=WRITE_ONLY,REPARTITION_SKIP_CUTOVER=true,REPARTITION_SKIP_CLEANUP=true) */";

    final String hint3 =
        " /*+TDDL:CMD_EXTRA(REPARTITION_SKIP_CLEANUP=true) */";

    protected final String dmlHintStr =
        " /*+TDDL:cmd_extra(PLAN_CACHE=false,DML_SKIP_DUPLICATE_CHECK_FOR_PK=FALSE,DML_USE_RETURNING=FALSE)*/ ";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testStayAtDeleteOnly() {
        String tableName = "modify_pk_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` drop primary key, add primary key(b)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        // 更新 delete only 下，UPDATE 在GSI表上还是 UPDATE
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 0);

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

        //预期：select + delete + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));
        assertTraceContains(trace, "DELETE", 2);
    }

    @Test
    public void testStayAtWriteOnly() {
        String tableName = "modify_pk_test_tbl2" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` modify a int", tableName);
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
    public void testStayAtCutOver() {
        String tableName = "modify_pk_test_tbl3" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint3 + String.format("alter table `%s` modify a int", tableName);
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
        String tableName = "modify_pk_test_tbl4" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add global index modify_sk_test_gsi(a, b, c) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` modify a int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        // 更新 delete only 下，UPDATE 在GSI表上还是 UPDATE
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 0);

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
        String tableName = "modify_pk_test_tbl5" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add global index modify_sk_test_gsi2(a, b, c) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` modify a int", tableName);
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
    public void testStayAtCutOverWithGsi() {
        String tableName = "modify_pk_test_tbl6" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add global index modify_sk_test_gsi3(a, b, c) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint3 + String.format("alter table `%s` modify a int", tableName);
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
    public void testStayAtDeleteOnlyWithImplicitKey() {
        String tableName = "modify_im_pk_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint, unique index local_idx(a)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` add primary key(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(1));

        //因为是delete only状态，所以预期GSI表包含DELETE流量
        // 更新 delete only 下，UPDATE 在GSI表上还是 UPDATE
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 0);

        //因为是delete only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);

        //预期：select 3 + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(4));

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
        Assert.assertThat(trace.size(), is(4));

        //预期：select + delete + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));
        assertTraceContains(trace, "DELETE", 1);
    }

    @Test
    public void testStayAtWriteOnlyWithImplicitKey() {
        String tableName = "modify_im_pk_test_tbl2" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint, unique index local_idx(a)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` add primary key(a)", tableName);
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

        //预期：select 3 + insert + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(5));

        //预期：select 3 + delete + delete + insert + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 2);
        assertTraceContains(trace, "INSERT", 2);

        //预期：select 3+ insert + insert
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
        Assert.assertThat(trace.size(), is(6));
        assertTraceContains(trace, "DELETE", 1);
    }

    @Test
    public void testStayAtCutOverWithImplicitKey() {
        String tableName = "modify_im_pk_test_tbl3" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint, unique index local_idx(a)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint3 + String.format("alter table `%s` add primary key(a)", tableName);
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
        Assert.assertThat(trace.size(), is(5));
        assertTraceContains(trace, "DELETE", 2);
    }

    @Test
    public void testStayAtDeleteOnlyWithImplicitPkGsi() {
        String tableName = "modify_im_pk_test_tbl4" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add unique global index modify_pk_test_gsi(a, b, c) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint1 + String.format("alter table `%s` modify a int", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('456', 4, 5)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        List<List<String>> trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(2));

        // 因为是delete only状态，所以预期GSI表包含DELETE流量
        // 更新 delete only 下，UPDATE 在GSI表上还是 UPDATE
        sql = String.format("UPDATE %s SET b = 2 WHERE a = '123'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 0);

        //因为是delete only状态，所以预期GSI表和主表都包含DELETE流量
        sql = String.format("DELETE FROM %s WHERE a = '234'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);

        //预期：select 3+ select uk + insert 2
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(6));

        //预期：select + delete + delete + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);
        assertTraceContains(trace, "INSERT", 2);

        //预期：select 3+ select uk + insert2
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(6));

        //预期：select + delete + insert
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(6));
        assertTraceContains(trace, "DELETE", 0);
    }

    @Test
    public void testStayAtWriteOnlyWithImplicitPkGsi() {
        String tableName = "modify_im_pk_test_tbl5" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint, unique index (a)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add unique global index modify_pk_test_gsi2(a, b, c) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint2 + String.format("alter table `%s` add primary key(a)", tableName);
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

        //预期：select3 + select uk + insert 2+ insert 2
        sql = String.format("INSERT IGNORE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(8));

        //预期：select + delete + delete + insert + insert
        sql = String.format("INSERT INTO %s VALUES ('567', 5, 6) ON DUPLICATE KEY UPDATE a = '678'", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        assertTraceContains(trace, "DELETE", 4);
        assertTraceContains(trace, "INSERT", 4);

        //预期：select3 + select uk + insert2 + insert2
        sql = String.format("REPLACE INTO %s VALUES ('567', 5, 6)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(8));

        //预期：select + delete + insert + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = checkTraceRowCount(is(12));
        assertTraceContains(trace, "DELETE", 4);
    }

    @Test
    public void testStayAtCutOverWithImplicitPkGsi() {
        String tableName = "modify_im_pk_test_tbl6" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint, unique index(a)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("insert into table `%s` values('123', 1, 2), ('234', 2, 3)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format(
            "alter table `%s` add unique global index modify_pk_test_gsi3(a, b, c) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = hint3 + String.format("alter table `%s` add primary key(a)", tableName);
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
        // DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY = true : primary(1) + gsi(1) + insert(4)
        // DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY = false : gsi(1) + insert(4)
        checkTraceRowCount(Matchers.lessThanOrEqualTo(6));

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
        // DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY = true : primary(1) + gsi(1) + insert(4)
        // DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY = false : gsi(1) + insert(4)
        checkTraceRowCount(Matchers.lessThanOrEqualTo(6));

        //预期：select + delete + insert + replace
        sql = String.format("INSERT IGNORE INTO %s VALUES ('678', 6 ,7)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dmlHintStr + sql);
        sql = String.format("REPLACE INTO %s VALUES ('678', 7, 8)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "trace" + dmlHintStr + sql);
        trace = getTrace(tddlConnection);
        Assert.assertThat(trace.size(), is(10));
        assertTraceContains(trace, "DELETE", 4);
    }

    protected void assertTraceContains(List<List<String>> trace, String targetStr, int count) {
        int c = 0;
        for (List<String> item : trace) {
            for (String s : item) {
                if (StringUtils.containsIgnoreCase(s, targetStr + " ")) {
                    c++;
                }
            }
        }
        //make sure now() is pushed down, instead of logical execution
        org.junit.Assert.assertEquals(count, c);
    }
}

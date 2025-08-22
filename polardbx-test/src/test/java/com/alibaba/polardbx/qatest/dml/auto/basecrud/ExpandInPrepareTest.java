package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class ExpandInPrepareTest extends BaseTestCase {

    @Test
    public void testExpandInSql() {
        String sql =
            " /*+TDDL:ENABLE_IN_TO_EXPAND_IN=true ENABLE_IN_TO_UNION_ALL=true IN_TO_UNION_STRICT_MODE=2 index(select_base_four_multi_db_multi_tb, primary)*/select * from select_base_four_multi_db_multi_tb where integer_test in (?,?,?,?,?,?,?) order by pk limit 10;";
        List<Object> params = new ArrayList<>();
        params.add("3");
        params.add("5");
        params.add("18");
        params.add("20");
        params.add("21");
        params.add("26");
        params.add("33");

        assertServerPrepareTest(getMysqlConnection(), getPolardbxConnection(), sql, params);

        sql =
            " /*+TDDL:ENABLE_IN_TO_EXPAND_IN=true ENABLE_IN_TO_UNION_ALL=true IN_TO_UNION_STRICT_MODE=2 index(select_base_four_multi_db_multi_tb, primary)*/select * from select_base_four_multi_db_multi_tb "
                + "where integer_test in (?,?,?,?,?,?) and varchar_test in (?,?,?,?,?,?) order by pk limit 10;";
        params = new ArrayList<>();
        params.add("18");
        params.add("26");
        params.add("33");
        params.add("3");
        params.add("20");
        params.add("21");
        params.add("word23");
        params.add("tt");
        params.add("zhuoxue");
        params.add("kisfe");
        params.add("sfdeiekd");
        params.add("einoejk");

        assertServerPrepareTest(getMysqlConnection(), getPolardbxConnection(), sql, params);
    }

    @Test
    public void testExpandInWithoutUnionAll() {
        String sql =
            " /*+TDDL:IN_TO_UNION_ALL_THRESHOLD=3 ENABLE_IN_TO_EXPAND_IN=true ENABLE_IN_TO_UNION_ALL=true IN_TO_UNION_STRICT_MODE=2 index(select_base_four_multi_db_multi_tb, primary)*/select * from select_base_four_multi_db_multi_tb where integer_test in (?,?,?,?,?,?,?) order by pk limit 10;";
        List<Object> params = new ArrayList<>();
        params.add("3");
        params.add("5");
        params.add("18");
        params.add("20");
        params.add("21");
        params.add("26");
        params.add("33");

        assertServerPrepareTest(getMysqlConnection(), getPolardbxConnection(), sql, params);

        sql =
            " /*+TDDL:IN_TO_UNION_ALL_THRESHOLD=3 ENABLE_IN_TO_EXPAND_IN=true ENABLE_IN_TO_UNION_ALL=true IN_TO_UNION_STRICT_MODE=2 index(select_base_four_multi_db_multi_tb, primary)*/select * from select_base_four_multi_db_multi_tb "
                + "where integer_test in (?,?,?,?,?,?) and varchar_test in (?,?,?,?,?,?) order by pk limit 10;";
        params = new ArrayList<>();
        params.add("18");
        params.add("26");
        params.add("33");
        params.add("3");
        params.add("5");
        params.add("20");
        params.add("21");
        params.add("word23");
        params.add("NULL");
        params.add("kisfe");
        params.add("sfdeiekd");
        params.add("einoejk");

        assertServerPrepareTest(getMysqlConnection(), getPolardbxConnection(), sql, params);
    }

    private void assertServerPrepareTest(
        Connection mysqlConnection,
        Connection tddlConnection,
        String sql,
        List<Object> params) {
        DataValidator.selectContentSameAssert(sql, params, mysqlConnection, tddlConnection);
    }
}

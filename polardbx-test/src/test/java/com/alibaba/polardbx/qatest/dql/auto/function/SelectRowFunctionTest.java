package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.sqlMayErrorAssert;

public class SelectRowFunctionTest extends AutoReadBaseTestCase {

    private List<String> rets = Lists.newArrayList("-100", "0.1", null, "'a'", "'b'", "100", "'f'");

    private String equalSql = "select %s = %s";
    private String nullSafeEqualSql = "select %s <=> %s";
    private String lessSql = "select %s < %s";
    private String lessThanSql = "select %s <= %s";
    private String greatSql = "select %s > %s";
    private String greaterThanSql = "select %s >= %s";
    private String inSql = "select %s in (%s)";
    private String notInSql = "select %s not in (%s)";

    private List<String> templates = Lists.newArrayList(
        equalSql, nullSafeEqualSql, lessSql, lessThanSql, greatSql, greaterThanSql, inSql, notInSql);

    @Test
    public void testSameRowLength() {
        for (int i = 1; i < 10; i++) {
            String left = generateRandomStr(i);
            String right = generateRandomStr(i);
            for (String template : templates) {
                String sql = String.format(template, left, right);
                selectContentSameAssert(sql, new ArrayList<>(), mysqlConnection, tddlConnection);
            }
        }
    }

    @Test
    public void testInNull() {
        selectContentSameAssert("select (1,2,3,4) in ((null,2,3,4), (1,3,4,2))", new ArrayList<>(), mysqlConnection,
            tddlConnection);
        selectContentSameAssert("select (1,2,null,4) in ((1,2,null,4), (1,3,4,2))", new ArrayList<>(), mysqlConnection,
            tddlConnection);
        selectContentSameAssert("select (1,2,null,4) in ((1,2,3,4), (1,3,4,2))", new ArrayList<>(), mysqlConnection,
            tddlConnection);
        selectContentSameAssert("select (1,2,null,4) in ((1,null,3,4), (1,3,4,2))", new ArrayList<>(), mysqlConnection,
            tddlConnection);
        selectContentSameAssert("select (1,2,3,4) in ((1, null,3,null), (1,3,4,2))", new ArrayList<>(), mysqlConnection,
            tddlConnection);
        selectContentSameAssert("select (1,2,3,4) in ((1, null,3,null), (1,3,4,2), (1,2,3,4))", new ArrayList<>(),
            mysqlConnection, tddlConnection);
        selectContentSameAssert("select 1 in (2,null,3)", new ArrayList<>(), mysqlConnection, tddlConnection);
        selectContentSameAssert("select (1,2,3,4) in ((null,2,3,4), (1,3,4,2))", new ArrayList<>(), mysqlConnection,
            tddlConnection);
        selectContentSameAssert("select null in (1,null,3)", new ArrayList<>(), mysqlConnection, tddlConnection);
    }

    @Test
    public void testDifferentRowLength() {
        String left = generateRandomStr(2);
        String right = generateRandomStr(3);
        for (String template : templates) {
            String sql = String.format(template, left, right);
            sqlMayErrorAssert(sql, tddlConnection, "ERR-CODE");
        }
    }

    private String generateRandomStr(int rowLength) {
        Random random = new Random();
        StringBuffer buffer = new StringBuffer();
        buffer.append("(");
        List<String> targets = new ArrayList<>();
        for (int i = 0; i < rowLength; i++) {
            targets.add(rets.get(random.nextInt(rets.size())));
        }
        buffer.append(targets.stream().collect(Collectors.joining(",")));
        buffer.append(")");
        return buffer.toString();
    }

}

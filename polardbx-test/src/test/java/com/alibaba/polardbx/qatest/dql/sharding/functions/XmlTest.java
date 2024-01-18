package com.alibaba.polardbx.qatest.dql.sharding.functions;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableName;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class XmlTest extends ReadBaseTestCase {
    private final String tableName = "test_xml";

    @Before
    public void prepareData() {
        JdbcUtil.executeSuccess(mysqlConnection, "drop table if exists " + tableName);
        JdbcUtil.executeSuccess(mysqlConnection, "create table " + tableName + " (a varchar(255));");
        JdbcUtil.executeSuccess(mysqlConnection,
            "insert into " + tableName + " (a) values (\"<a><b>ccc</b><d></d></a>\");");
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists " + tableName);
        JdbcUtil.executeSuccess(tddlConnection, "create table " + tableName + " (a varchar(255));");
        JdbcUtil.executeSuccess(tddlConnection,
            "insert into " + tableName + " (a) values (\"<a><b>ccc</b><d></d></a>\");");

    }

    @After
    public void deleteData() {
        JdbcUtil.executeSuccess(mysqlConnection, "drop table if exists " + tableName);
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists " + tableName);
    }

    public XmlTest() {
    }

    @Test
    public void updateXML() {
        String[] sqls = {
            "SELECT UpdateXML('<a><b>ccc</b><d></d></a>', '/a', '<e>fff</e>') AS val1",
            "SELECT UpdateXML('<a><b>ccc</b><d></d></a>', '/b', '<e>fff</e>') AS val2",
            "SELECT UpdateXML('<a><b>ccc</b><d></d></a>', '//b', '<e>fff</e>') AS val3",
            "SELECT UpdateXML('<a><b>ccc</b><d></d></a>', '/a/d', '<e>fff</e>') AS val4",
            "SELECT UpdateXML('<a><d></d><b>ccc</b><d></d></a>', '/a/d', '<e>fff</e>') AS val5",
            "SELECT UpdateXML('<a>111<b:c>222<d>333</d><e:f>444</e:f></b:c></a>',  '//b:c', '<g:h>555</g:h>');",
            "SELECT UpdateXML('', '/a', '');",
        };
        for (String sql : sqls) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        //pushdown
        String sql = "SELECT UpdateXML(a, '/a', '<e>fff</e>') from " + tableName + ";";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        //error
        sql =
            "SELECT UpdateXML('<a><b><c>w</c><b>x</b><d>y</d>z</b></a>', '//b[1]', '<e>fff</e>');";
        JdbcUtil.executeFaied(tddlConnection, sql, "UpdateXML index visit is not support");

        sql = "SELECT UpdateXML('<a><b>X</b><b>Y</b></a>', '$@i', '<e>fff</e>');";
        JdbcUtil.executeFaied(tddlConnection, sql, "UpdateXML variable visit is not support");

        sql = "SELECT UpdateXML('', '', '');";
        JdbcUtil.executeFaied(tddlConnection, sql, "XPATH syntax error:");

    }

    @Test
    public void extractValue() {
        String[] sqls = {
            "SELECT ExtractValue('<a>ccc<b>ddd</b></a>', '/a') AS val1",
            "SELECT ExtractValue('<a>ccc<b>ddd</b></a>', '/a/b') AS val2",
            "SELECT ExtractValue('<a>ccc<b>ddd</b></a>', '//b') AS val3",
            "SELECT ExtractValue('<a>ccc<b>ddd</b></a>', '/b') AS val4",
            "SELECT ExtractValue('<a>ccc<b>ddd</b><b>eee</b></a>', '//b') AS val5",
            "SELECT ExtractValue(\n"
                + "    '<a><b c=\"1\">X</b><b c=\"2\">Y</b></a>',\n"
                + "    'a/b'\n"
                + "    ) AS result;",
            "SELECT ExtractValue('<a><b>x</b><c>y</c></a>','/a/child::b');",
            "SELECT ExtractValue('<a><b>x</b><c>y</c></a>','/a/child::*');",
            "SELECT ExtractValue('<a><b/></a>', 'count(/a/b)');",
            "SELECT ExtractValue('<a><c/></a>', 'count(/a/b)');",
            "SELECT\n"
                + "    EXTRACTVALUE('<cases><case/></cases>', '/cases/case') \n"
                + "    AS 'Empty Example',\n"
                + "    EXTRACTVALUE('<cases><case/></cases>', 'count(/cases/case)') \n"
                + "    AS 'count() Example';",
            "SELECT ExtractValue('', '/a');"
        };
        for (String sql : sqls) {
            selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
        }

        //pushdown
        String sql = "SELECT ExtractValue(a, '/a') from " + tableName + ";";
        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);

        sql = String.format("SELECT ExtractValue('<a><b><c>w</c><b>x</b><d>y</d>z</b></a>', '//b[1]');");
        JdbcUtil.executeFaied(tddlConnection, sql, "ExtractValue index visit is not support");

        sql = String.format("SELECT ExtractValue('<a><b>X</b><b>Y</b></a>', '$@i');");
        JdbcUtil.executeFaied(tddlConnection, sql, "ExtractValue variable visit is not support");

        sql = String.format("SELECT ExtractValue('', '');");
        JdbcUtil.executeFaied(tddlConnection, sql, "XPATH syntax error:");
    }

}

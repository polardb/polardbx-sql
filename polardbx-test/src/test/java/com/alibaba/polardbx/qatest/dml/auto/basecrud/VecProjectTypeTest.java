package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VecProjectTypeTest extends AutoCrudBasedLockTestCase {
    private static String TABLE_NAME = "test_vec_project_type";

    private static String CREATE_TABLE = "CREATE TABLE %s (\n"
        + "        `c1` text NOT NULL,\n"
        + "        `c2` int(11) NOT NULL DEFAULT '0',\n"
        + "        `c3` int(11) NOT NULL DEFAULT '0',\n"
        + "        `c4` int(11) NOT NULL DEFAULT '0',\n"
        + "        `c5` varchar(10) DEFAULT NULL,\n"
        + "        `c6` int(11) NOT NULL DEFAULT '0',\n"
        + "        `c7` double DEFAULT NULL,\n"
        + "        `c8` varchar(100) NOT NULL,\n"
        + "        `c9` int(11) NOT NULL DEFAULT '0'\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8\n";
    private static String PARTITION_INFO = "PARTITION BY KEY(`_drds_implicit_id_`)\n" + "PARTITIONS 16;\n";

    @Test
    public void testVecProjectResultType() {
        String updateData =
            String.format("/*+TDDL:cmd_extra(ENABLE_EXPRESSION_VECTORIZATION=true)*/ update `%s` set `c9` = 1,"
                + "`c1` = x'32303234303132353131333530375F34',"
                + "`c2` = 4,`c4` = 20,`c5` = x'',"
                + "`c3` = 114174,`c6` = 2493,`c7` = 42.8,"
                + "`c8` = x'476A6473315F417667' "
                + "WHERE `c9` = 1 and `c1` = x'32303234303132353131333530375F34' "
                + "and `c2` = 4 and `c4` = 20 and `c5` = x'' and `c3` = 114174 "
                + "and `c6` = 2493 and abs(`c7` - 42.0) < 0.000001 and `c8` = x'476A6473315F417667' "
                + "limit 1;\n", TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, updateData);
        JdbcUtil.executeSuccess(mysqlConnection, updateData);
        DataValidator.selectContentSameAssert(String.format("select * from %s order by c1", TABLE_NAME), null,
            mysqlConnection, tddlConnection);
    }

    @Before
    public void prepareData() {
        dropTable();

        // create table
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE + PARTITION_INFO, TABLE_NAME));
        String addGlobalIndex =
            String.format("alter table %s add GLOBAL INDEX `g_idx_temp_1` (`c3`, `c4`, `c5`, `c6`)\n"
                + "PARTITION BY KEY(`c3`,`c4`,`c5`,`c6`,`_drds_implicit_id_`) PARTITIONS 16", TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, addGlobalIndex);
        JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE, TABLE_NAME));

        // insert data twice
        String insertData =
            String.format("insert into %s values('20240125113507_4', 4, 114174, 20, '', 2493, 42, 'Gjds1_Avg',1);",
                TABLE_NAME);
        JdbcUtil.executeSuccess(tddlConnection, insertData);
        JdbcUtil.executeSuccess(tddlConnection, insertData);
        JdbcUtil.executeSuccess(mysqlConnection, insertData);
        JdbcUtil.executeSuccess(mysqlConnection, insertData);
    }

    @After
    public void dropTable() {
        // drop table
        String dropTable = "drop table if exists " + TABLE_NAME;
        JdbcUtil.executeSuccess(tddlConnection, dropTable);
        JdbcUtil.executeSuccess(mysqlConnection, dropTable);
    }
}

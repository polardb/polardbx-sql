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
public class CdasSyntaxTest extends DDLBaseNewDBTestCase {
    private static final String autoDbName = "test_syntax_auto_db";
    private static final String drdsDbName = "test_syntax_drds_db";
    private static final String drdsDbNameExist = "test_syntax_drds_db_exist";
    private static final String createSourceDbSql = "create database {0} mode=drds";
    private static final String createTargetDbSql = "create database {0} mode=auto";
    private static final String dropDbSql = "drop database if exists {0} ";

    private static final String createTableSql = "create table {0} (id int) broadcast";

    @Before
    public void prepareDb() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, drdsDbName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, autoDbName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, drdsDbNameExist));

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createSourceDbSql, drdsDbName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createSourceDbSql, drdsDbNameExist));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createTargetDbSql, autoDbName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format("use {0}", drdsDbName));

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createTableSql, "tb0"));

        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(createTableSql, "tb1"));
    }

    @After
    public void dropDb() {
        useDb(tddlConnection, tddlDatabase1);
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, drdsDbName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, autoDbName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
            .format(dropDbSql, drdsDbNameExist));
    }

    @Test
    public void testWrongSyntax() {
        //include, exclude should not exist at the same time
        final String sql1 = "create database {0} like {1} exclude=tb0 include=tb1";
        //include/exclude should be declared at the end of clause
        final String sql2 = "create database {0} as {1} exclude=tb0 mode=auto";
        //only cdas/cdlike can use these options
        final String sql3 = "create database {0} exclude=tb0";
        final String sql4 = "create database {0} include=tb0";
        final String sql5 = "create database {0} dry_run=true";
        final String sql6 = "create database {0} lock=true";
        final String sql7 = "create database {0} create_tables = false";
        final String tempDbName0 = "tempdb0";
        final String tempDbName1 = "tempdb1";

        JdbcUtil.executeUpdateFailed(tddlConnection, MessageFormat.format(sql1, tempDbName0, tempDbName1),
            "syntax error");
        JdbcUtil.executeUpdateFailed(tddlConnection, MessageFormat.format(sql2, tempDbName0, tempDbName1),
            "syntax error");
        JdbcUtil.executeUpdateFailed(tddlConnection, MessageFormat.format(sql3, tempDbName0), "INVALID TOKEN");
        JdbcUtil.executeUpdateFailed(tddlConnection, MessageFormat.format(sql4, tempDbName0), "INVALID TOKEN");
        JdbcUtil.executeUpdateFailed(tddlConnection, MessageFormat.format(sql5, tempDbName0), "INVALID TOKEN");
        JdbcUtil.executeUpdateFailed(tddlConnection, MessageFormat.format(sql6, tempDbName0), "INVALID TOKEN");
        JdbcUtil.executeUpdateFailed(tddlConnection, MessageFormat.format(sql7, tempDbName0), "INVALID TOKEN");
    }

    @Test
    public void testWrongSemantic() {
        //create already exist database
        /**
         * to test:
         * 1. create already exist database
         * 2. newly created db should not have the same name with old db
         * 3. reference db not exist
         * */
        final String sql1 = "create database {0}" + " like {1}";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql1, drdsDbNameExist, drdsDbName),
            "already exists");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql1, drdsDbName, drdsDbName),
            "should not have the same name");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql1, "temp_name", "temp_name2"),
            "doesn't exist");

        /**
         * to test:
         * 1. option: create_tables=false, but new database not exist
         * 2. option: create_tables=false, but tables in new database defect
         * */
        final String sql2 = "create database {0} " + " like {1} create_tables=false;";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql2, "temp_name", drdsDbName),
            "not found");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql2, autoDbName, drdsDbName),
            "not found");

        /**
         * to test
         * 1. reference db should be drds mode
         * 2. included tables should all exists in reference db
         * */
        final String sql3 = "create database {0} " + " like {1} ";
        final String sql4 = "create database {0} " + " like {1} include={1}";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql3, "temp_name", autoDbName),
            "must be 'drds mode'");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql4, "temp_name", drdsDbName, "tbnone"),
            "not exist");

        /**
         * to test
         * 1. new db must in auto mode
         * 2. new db's charset/collate will be same as the reference db,
         *    so user should not set charset/collate on new db
         * */
        final String sql5 = "create database {0} " + " like {1} mode=drds";
        final String sql6 = "create database {0} " + " like {1} mode=auto charset=utf8";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql5, "temp_name", drdsDbName),
            "must be auto/partitioning mode");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql6, "temp_name", drdsDbName),
            "not allowed to define charset/collate");
    }

    @Test
    public void testWrongSemanticDryRun() {
        //create already exist database
        /**
         * to test:
         * 1. newly created db should not have the same name with old db
         * 2. reference db not exist
         * */
        final String sql1 = "create database {0}" + " like {1} dry_run=true";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql1, drdsDbName, drdsDbName),
            "should not have the same name");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql1, "temp_name", "temp_name2"),
            "doesn't exist");

        /**
         * to test:
         * 1. option: create_tables=false, but new database not exist
         * 2. option: create_tables=false, but tables in new database defect
         * */
        final String sql2 = "create database {0} " + " like {1} create_tables=false dry_run=true;";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql2, "temp_name", drdsDbName),
            "not found");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql2, autoDbName, drdsDbName),
            "not found");

        /**
         * to test
         * 1. reference db should be drds mode
         * 2. included tables should all exists in reference db
         * */
        final String sql3 = "create database {0} " + " like {1} dry_run=true";
        final String sql4 = "create database {0} " + " like {1} dry_run=true include={1}";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql3, "temp_name", autoDbName),
            "must be in 'drds mode'");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql4, "temp_name", drdsDbName, "tbnone"),
            "not exist");

        /**
         * to test
         * 1. new db must in auto mode
         * 2. new db's charset/collate will be same as the reference db,
         *    so user should not set charset/collate on new db
         * */
        final String sql5 = "create database {0} " + " like {1} mode=drds dry_run=true";
        final String sql6 = "create database {0} " + " like {1} mode=auto charset=utf8 dry_run=true";
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql5, "temp_name", drdsDbName),
            "must be auto/partitioning mode");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            MessageFormat.format(sql6, "temp_name", drdsDbName),
            "not allowed to define charset/collate");
    }
}

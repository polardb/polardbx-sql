package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

@FileStoreIgnore
public class InformationSchemaTableConstraintTest extends DDLBaseNewDBTestCase {

    private static final String TEST_TB1 = "np_test";

    private static final String TEST_TB2 = "dp_test";

    private static final String TEST_TB3 = "sg_test";
    private static final String CREATE_TABLE_FORMAT_NO_PRIMARY = "CREATE TABLE if not exists `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`integer_test` int(11) NOT NULL,\n"
        + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
        + "\tKEY (`pk`, `integer_test`),\n"
        + "\tUNIQUE KEY (`varchar_test`, `integer_test`)\n"
        + ")";

    private static final String CREATE_TABLE_FORMAT_DUP_PRIMARY = "CREATE TABLE if not exists `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`integer_test` int(11) NOT NULL,\n"
        + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`, `integer_test`),\n"
        + "\tUNIQUE KEY (`varchar_test`, `integer_test`)\n"
        + ")";

    private static final String CREATE_TABLE_FORMAT_SINGLE = "CREATE TABLE if not exists `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`integer_test` int(11) NOT NULL,\n"
        + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`, `integer_test`),\n"
        + "\tUNIQUE KEY (`varchar_test`, `integer_test`)\n"
        + ") %s";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Override
    protected Connection getTddlConnection1() {
        if (tddlConnection == null) {
            String database1 = getTestDBName("table_constraint_");
            String myDatabase1 = database1;
            this.tddlConnection = createTddlDb(database1);
            this.mysqlConnection = createMysqlDb(myDatabase1);
            this.tddlDatabase1 = database1;
            this.mysqlDatabase1 = myDatabase1;
            this.infomationSchemaDbConnection = getMysqlConnection("information_schema");
        }
        return tddlConnection;
    }

    @Before
    public void prepareDb() {
    }

    @After
    public void clearDb() {
        cleanDataBase();
    }

    @Test
    public void testTableConstraint() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_FORMAT_NO_PRIMARY, TEST_TB1));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(CREATE_TABLE_FORMAT_NO_PRIMARY, TEST_TB1));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_FORMAT_DUP_PRIMARY, TEST_TB2));
        JdbcUtil.executeUpdateSuccess(mysqlConnection, String.format(CREATE_TABLE_FORMAT_DUP_PRIMARY, TEST_TB2));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_FORMAT_SINGLE, TEST_TB3, "single"));
        JdbcUtil.executeSuccess(mysqlConnection, String.format(CREATE_TABLE_FORMAT_SINGLE, TEST_TB3, ""));
        String sql = String.format(
            "select CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME , CONSTRAINT_TYPE"
                + " from information_schema.TABLE_CONSTRAINTS where constraint_schema='%s'",
            tddlDatabase1);

        DataValidator.selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }
}

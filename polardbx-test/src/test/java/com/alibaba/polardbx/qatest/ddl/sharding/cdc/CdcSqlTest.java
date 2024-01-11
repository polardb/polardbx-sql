package com.alibaba.polardbx.qatest.ddl.sharding.cdc;

import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * created by ziyang.lb
 **/
public class CdcSqlTest extends BaseTestCase {

    private static final String CREATE_SQL = "CREATE TABLE `t_show_create_test` (\n"
        + "\t`tinyintr` tinyint(4) NOT NULL,\n"
        + "\t`tinyintr_1` tinyint(1) DEFAULT NULL,\n"
        + "\t`tinyintr_3` tinyint(3) DEFAULT NULL\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = gbk";

    private static final String DROP_SQL = "drop table if exists t_show_create_test";

    private static final String SHOW_SQL =
        "/!+TDDL:cmd_extra(SHOW_IMPLICIT_ID=true) */show create table drds_polarx2_part_qatest_app.t_show_create_test";

    private static final String CHECK_SQL = "CREATE TABLE `t_show_create_test` (\n"
        + "\t`tinyintr` tinyint(4) NOT NULL,\n"
        + "\t`tinyintr_1` tinyint(1) DEFAULT NULL,\n"
        + "\t`tinyintr_3` tinyint(3) DEFAULT NULL,\n"
        + "\t`_drds_implicit_id_` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "\tPRIMARY KEY (`_drds_implicit_id_`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = gbk";

    @Test
    public void testShowImplicitId() throws SQLException {
        try (Connection connection = getPolardbxConnection("drds_polarx2_part_qatest_app")) {
            Statement stmt = connection.createStatement();
            stmt.execute(DROP_SQL);
            stmt.execute(CREATE_SQL);

            ResultSet rs = stmt.executeQuery(SHOW_SQL);
            if (rs.next()) {
                String sql = rs.getString(2);
                if (isMySQL80()) {
                    sql = sql.replace("`tinyintr` tinyint NOT NULL,", "`tinyintr` tinyint(4) NOT NULL,");
                    sql = sql.replace("`tinyintr_3` tinyint DEFAULT NULL,", "`tinyintr_3` tinyint(3) DEFAULT NULL,");
                    sql = sql.replace("`_drds_implicit_id_` bigint NOT NULL AUTO_INCREMENT,",
                        "`_drds_implicit_id_` bigint(20) NOT NULL AUTO_INCREMENT,");
                }
                Assert.assertEquals(CHECK_SQL, sql);
            }
        }
    }
}

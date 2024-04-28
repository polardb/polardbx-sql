package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.google.common.truth.Truth.assertThat;

@FileStoreIgnore
public class InformationSchemaPlanCacheTest extends DDLBaseNewDBTestCase {

    private static final String TEST_TB = "np_test";

    private static final String CREATE_TABLE_FORMAT_NO_PRIMARY = "CREATE TABLE if not exists `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`integer_test` int(11) NOT NULL,\n"
        + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
        + "\tKEY (`pk`, `integer_test`),\n"
        + "\tUNIQUE KEY (`varchar_test`, `integer_test`)\n"
        + ")";

    private static final String TEST_SQL =
        " select count(1) from %s where integer_test=%s";
    private static final String COUNT_BY_SCHEMA =
        "/*+TDDL:cmd_extra()*/ select count(1) from information_schema.plan_cache where SCHEMA_NAME='%s'";

    private static final String SELECT_BY_SCHEMA =
        "/*+TDDL:cmd_extra()*/ select PARAMETER from information_schema.plan_cache where SCHEMA_NAME='%s' and %s";
    private static final String GROUP_COUNT_BY_SCHEMA =
        "/*+TDDL:cmd_extra()*/ select SCHEMA_NAME,count(1) from information_schema.plan_cache group by SCHEMA_NAME";

    private static final String COUNT_OF_CAPACITY =
        "/*+TDDL:cmd_extra()*/ select sum(CACHE_KEY_CNT) from information_schema.plan_cache_capacity where SCHEMA_NAME='%s'";
    protected String CLEAR_CACHE = "clear plancache";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void prepareCatalog() {
        JdbcUtil.executeUpdateSuccess(getTddlConnection1(), String.format(CREATE_TABLE_FORMAT_NO_PRIMARY, TEST_TB));
    }

    @After
    public void clearDb() {
        cleanDataBase();
    }

    private long getRealCapacity(String schema) {
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(COUNT_OF_CAPACITY, schema), getTddlConnection1())) {
            Assert.assertTrue(rs.next());
            return rs.getLong(1);
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        return -1;
    }

    @Test
    public void testPlanCacheSize() {
        try {
            ResultSet rs;

            String para1 = "123456";
            String para2 = "987654";
            //test parameter
            JdbcUtil.executeSuccess(getTddlConnection1(), CLEAR_CACHE);

            JdbcUtil.executeQuery(String.format(TEST_SQL, TEST_TB, para1), getTddlConnection1());
            JdbcUtil.executeQuery(String.format(TEST_SQL, TEST_TB, para2), getTddlConnection1());
            rs = JdbcUtil.executeQuery(String.format(SELECT_BY_SCHEMA, tddlDatabase1, 2),
                getTddlConnection1());
            Assert.assertTrue(rs.next());
            assertThat(rs.getString("PARAMETER")).contains(para1);

            // test plan cache
            JdbcUtil.executeSuccess(getTddlConnection1(), CLEAR_CACHE);
            JdbcUtil.executeSuccess(getTddlConnection2(), CLEAR_CACHE);
            JdbcUtil.executeQuery(String.format(TEST_SQL, TEST_TB, para1), getTddlConnection1());

            rs = JdbcUtil.executeQuery(String.format(COUNT_BY_SCHEMA, tddlDatabase1), getTddlConnection1());
            Assert.assertTrue(rs.next());
            long count1 = rs.getLong(1);
            assertThat(count1).isEqualTo(getRealCapacity(tddlDatabase1));
            rs.close();

            rs = JdbcUtil.executeQuery(String.format(COUNT_BY_SCHEMA, tddlDatabase2), getTddlConnection1());
            Assert.assertTrue(rs.next());
            long count2 = rs.getLong(1);
            rs.close();
            assertThat(count2).isEqualTo(getRealCapacity(tddlDatabase2));

            rs = JdbcUtil.executeQuery(GROUP_COUNT_BY_SCHEMA, getTddlConnection2());
            boolean findDB1 = false;
            boolean findDB2 = false;
            while (rs.next()) {
                if (tddlDatabase1.equalsIgnoreCase(rs.getString(1))) {
                    assertThat(rs.getLong(2)).isEqualTo(getRealCapacity(tddlDatabase1));
                    findDB1 = true;
                }
                if (tddlDatabase2.equalsIgnoreCase(rs.getString(1))) {
                    assertThat(rs.getLong(2)).isEqualTo(getRealCapacity(tddlDatabase2));
                    findDB2 = true;
                }
            }
            rs.close();
            assertThat(findDB1).isTrue();
            assertThat(findDB2).isFalse();
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
    }
}

package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PrunerReaderTest extends AutoReadBaseTestCase {
    private static final String TABLE_NAME = "key_varchar";
    private static final String COLUMNAR_INDEX_NAME = "col_" + TABLE_NAME;

    private static final String CREATE_TABLE =
        "create table if not exists %s "
            + "(c1 varchar(8) CHARACTER SET utf8 COLLATE utf8_general_ci default null,"
            + "`pk` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "PRIMARY KEY (pk) ) partition by key(c1) partitions 16;\n";
    private static final String INSERT_DATA =
        "insert into %s(c1) values ('a'),('b'),(''),('世界'),( x'E4B896E7958C' ),('☺'),('12345678'),(null);\n";
    private static final String DROP_TABLE = "drop table if exists %s";

    @Before
    public void prepare() {
        dropTable();
        prepareData();
    }

    @Test
    public void testGlobalShowPruneTracer() throws SQLException {

        String sqlSelect =
            String.format("select c1 from %s force index (%s) where ((c1) = ( x'E4B896E7958C' )) order by c1;",
                TABLE_NAME, COLUMNAR_INDEX_NAME);
        ResultSet rs = JdbcUtil.executeQuery(sqlSelect, tddlConnection);
        Assert.assertEquals(2, count(rs));

        sqlSelect =
            String.format(
                "/*+ TDDL: ENABLE_INDEX_PRUNING=false*/ select c1 from %s force index (%s) where ((c1) = ( x'E4B896E7958C' )) ;",
                TABLE_NAME, COLUMNAR_INDEX_NAME);
        rs = JdbcUtil.executeQuery(sqlSelect, tddlConnection);
        Assert.assertEquals(2, count(rs));

        sqlSelect =
            String.format("select c1 from %s force index (%s) where ((c1) = ( '世界' )) order by c1;",
                TABLE_NAME, COLUMNAR_INDEX_NAME);
        rs = JdbcUtil.executeQuery(sqlSelect, tddlConnection);
        Assert.assertEquals(2, count(rs));

        sqlSelect =
            String.format("select c1 from %s force index (%s) where ((c1) = ( '12345678' )) order by c1;",
                TABLE_NAME, COLUMNAR_INDEX_NAME);
        rs = JdbcUtil.executeQuery(sqlSelect, tddlConnection);
        Assert.assertEquals(1, count(rs));

        sqlSelect =
            String.format(
                "select c1 from %s force index (%s) where ((c1) = CONCAT('1234', '5678')) order by c1;",
                TABLE_NAME, COLUMNAR_INDEX_NAME);
        rs = JdbcUtil.executeQuery(sqlSelect, tddlConnection);
        Assert.assertEquals(1, count(rs));
    }

    int count(ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        return count;
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
    }

    private void prepareData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_NAME));
        // create columnar index
        ColumnarUtils.createColumnarIndex(tddlConnection, COLUMNAR_INDEX_NAME, TABLE_NAME, "c1", "c1", 16);
    }

}

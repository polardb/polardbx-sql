package com.alibaba.polardbx.qatest.statistic.collect;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.qatest.statistic.collect.StatisticCollectionTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * ouput statistic report without judge
 *
 * @author fangwu
 */
public class DefaultStatisticCollectionTest extends StatisticCollectionTest {

    public DefaultStatisticCollectionTest(String schema, String tbl) {
        super(schema, tbl, "DefaultStatisticCollectionTest");
    }

    @Parameterized.Parameters(name = "{index}: [{0}] {1}")
    public static Iterable<Object[]> params() throws SQLException {
        List<Object[]> params = Lists.newLinkedList();
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            ResultSet rs = c.createStatement().executeQuery("show databases");
            Set<String> schemas = Sets.newHashSet();
            while (rs.next()) {
                schemas.add(rs.getString(1));
            }
            log.info("statistic collection test schemas:" + schemas);
            for (String schema : schemas) {
                Collection<String> tbls = getTables(schema);
                if (tbls.isEmpty()) {
                    continue;
                }
                if (SystemDbHelper.isDBBuildIn(schema)) {
                    continue;
                }

                for (String tbl : tbls) {
                    params.add(new Object[] {schema, tbl});
                }
            }
            log.info("statistic collection test case num:" + params.size());
            return params;
        }
    }

    private static Collection<String> getTables(String schema) {
        Collection<String> tbls = Sets.newHashSet();
        try (Connection c = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            c.setSchema(schema);
            c.createStatement().executeQuery("use " + schema);
            ResultSet resultSet = c.createStatement().executeQuery("show tables");
            while (resultSet.next()) {
                tbls.add(resultSet.getString(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return tbls;
    }
}

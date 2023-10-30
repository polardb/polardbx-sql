package com.alibaba.polardbx.qatest.dal;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.regex.Pattern;

public class VersionTest extends ReadBaseTestCase {
    @Test
    public void testSelectAllVersions() throws SQLException {
        final Set<String> expectNodeTypes = Sets.newHashSet("CN",
            "DN", "CDC", "GMS");
        String sql = "select polardb_version()";
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String nodeType = rs.getString("NODE_TYPE").toUpperCase();
                Assert.assertTrue("Unexpected node type: " + nodeType,
                    expectNodeTypes.remove(nodeType));
            }
        }
        Assert.assertTrue("Remain node_types: " + StringUtils.join(expectNodeTypes, ","),
            expectNodeTypes.isEmpty());
    }
}

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

public class VersionTest extends ReadBaseTestCase {

    @Test
    public void testSelectPolarDBVersions() throws SQLException {
        // some components may not exist
        final Set<String> requiredNodeTypes = Sets.newHashSet("Product", "CN",
            "DN", "GMS");
        final Set<String> optionalNodeTypes = Sets.newHashSet("CDC", "Columnar");
        String sql = "select polardb_version()";
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String nodeType = rs.getString("TYPE");
                // check field exists
                String version = rs.getString("VERSION");
                // check field exists
                String releaseDate = rs.getString("RELEASE_DATE");
                boolean isRequired = requiredNodeTypes.remove(nodeType);
                if (isRequired) {
                    continue;
                }
                boolean isOptional = optionalNodeTypes.remove(nodeType);
                Assert.assertTrue("Unexpected node type: " + nodeType, isOptional);
            }
        }
        Assert.assertTrue("Remain node_types: " + StringUtils.join(requiredNodeTypes, ","),
            requiredNodeTypes.isEmpty());
    }

}

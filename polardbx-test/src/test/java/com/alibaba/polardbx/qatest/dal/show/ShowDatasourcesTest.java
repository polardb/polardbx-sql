/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dal.show;

import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ShowDatasourcesTest extends ReadBaseTestCase {

    @Test
    public void testShowDatasources() throws SQLException {
        String sql = "show datasources";
        ResultSet rs = null;

        try {
            rs = JdbcUtil.executeQuery(sql, tddlConnection);
            boolean containsMetaDB = false;
            int expectId = 0;
            while (rs.next()) {
                int id = rs.getInt("ID");
                Assert.assertEquals("Incorrect ID", expectId, id);
                expectId++;
                String group = rs.getString("GROUP");
                if (group.equalsIgnoreCase(SystemDbHelper.DEFAULT_META_DB_NAME)) {
                    containsMetaDB = true;
                } else if (group.equalsIgnoreCase("INFORMATION_SCHEMA_SINGLE_GROUP")) {
                    Assert.fail("InformationSchema should not exist in normal datasource");
                }
            }

            if (!containsMetaDB) {
                Assert.fail("No MetaDB in datasource");
            }
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testShowDatasourcesInInformationSchema() throws SQLException {
        String sql = "show datasources";
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = getPolardbxConnection(SystemDbHelper.INFO_SCHEMA_DB_NAME);
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            boolean containsInformationSchema = false;
            String metaStorage = null;
            String informationSchemaStorage = null;
            int expectId = 0;
            while (rs.next()) {
                int id = rs.getInt("ID");
                Assert.assertEquals("Incorrect ID", expectId, id);
                expectId++;
                String group = rs.getString("GROUP");
                if (group.equalsIgnoreCase(SystemDbHelper.INFO_SCHEMA_DB_GROUP_NAME)) {
                    containsInformationSchema = true;
                    if (informationSchemaStorage == null) {
                        informationSchemaStorage = rs.getString("STORAGE_INST_ID");
                    } else {
                        Assert.assertEquals("Unexpected STORAGE_INST_ID for InformationSchema",
                            informationSchemaStorage, rs.getString("STORAGE_INST_ID"));
                    }
                } else if (group.equalsIgnoreCase(SystemDbHelper.DEFAULT_META_DB_NAME)) {
                    metaStorage = rs.getString("STORAGE_INST_ID");
                }
            }
            Assert.assertEquals("Unexpected STORAGE_INST_ID for InformationSchema",
                metaStorage, informationSchemaStorage);
            if (!containsInformationSchema) {
                Assert.fail("No InformationSchema in datasource");
            }
        } finally {
            JdbcUtil.close(stmt);
        }
    }
}

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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertWithMessage;

public class AsyncDDLBaseNewDBTestCase extends DDLBaseNewDBTestCase {

    protected static final Logger logger = LoggerFactory.getLogger(AsyncDDLBaseNewDBTestCase.class);

    protected String getTbNamePattern(String tableName) {
        try (Connection conn = getPolardbxConnection()) {
            return getTbNamePattern(tableName, conn);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail("Failed to get connection: " + e.getMessage());
        }
        return null;
    }

    public String getTbNamePattern(String tableName, Connection conn) {
        String sql = "show full rule from %s";
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(String.format(sql, tableName))) {
            if (rs.next()) {
                return rs.getString("TB_NAME_PATTERN");
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            Assert.fail("Failed to execute " + sql + ": " + e.getMessage());
        }
        return null;
    }

    // Extract a specified physical table name from logical table topology.

    protected String getPhysicalTableName(String tableSchema, String tableName) {
        return getPhysicalTableName(tableSchema, tableName, 0);
    }

    protected String getPhysicalTableName(String tableSchema, String tableName, int groupIndex) {
        return getPhysicalTableName(tableSchema, tableName, groupIndex, 0);
    }

    protected String getPhysicalTableName(String tableSchema, String tableName, int groupIndex, int tableIndex) {
        Map<String, List<String>> topology = getTopology(tableSchema, tableName);
        return getPhysicalTableName(groupIndex, tableIndex, topology);
    }

    protected String getPhysicalTableName(int groupIndex, int tableIndex, Map<String, List<String>> topology) {
        List<String> tableNames = getPhysicalTableNames(groupIndex, topology);
        if (tableNames != null && !tableNames.isEmpty()) {
            return tableNames.get(tableIndex);
        }
        return null;
    }

    protected List<String> getPhysicalTableNames(int groupIndex, Map<String, List<String>> topology) {
        List<String> groupNames = new ArrayList<>();
        if (!topology.isEmpty()) {
            for (String groupName : topology.keySet()) {
                groupNames.add(groupName);
            }
        }
        Collections.sort(groupNames);

        String targetGroup = groupNames.get(groupIndex);

        return topology.get(targetGroup);
    }

    protected String getPhysicalTableName(String tableSchema, String logicalTableName, String suffix,
                                          int groupIndex) {
        String physicalTableName = null;

        Map<String, List<String>> topology = getTopology(tableSchema, logicalTableName);
        if (topology == null || topology.isEmpty()) {
            return null;
        }

        if (groupIndex < 0) {
            for (List<String> tableNames : topology.values()) {
                for (String tableName : tableNames) {
                    if (tableName.endsWith(suffix)) {
                        physicalTableName = tableName;
                        break;
                    }
                }
            }
        } else {
            List<String> tableNames = getPhysicalTableNames(groupIndex, topology);
            if (tableNames != null && !tableNames.isEmpty()) {
                for (String tableName : tableNames) {
                    if (tableName.endsWith(suffix)) {
                        physicalTableName = tableName;
                        break;
                    }
                }
            }
        }

        return physicalTableName;
    }

    protected Map<String, List<String>> getTopology(String tableSchema, String logicalTableName) {
        String showTopology = "show topology from ";
        if (TStringUtil.isNotEmpty(tableSchema)) {
            showTopology += tableSchema + "." + logicalTableName;
        } else {
            showTopology += logicalTableName;
        }

        Map<String, List<String>> topology = new HashMap<>();

        try (Connection conn = getPolardbxConnection();
            PreparedStatement ps = conn.prepareStatement(showTopology);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String groupName = rs.getString(2);
                List<String> tableNames = topology.computeIfAbsent(groupName, k -> new ArrayList<>());
                tableNames.add(rs.getString(3));
            }
        } catch (SQLException e) {
            assertWithMessage("Failed to get topology: " + e.getMessage()).fail();
        }

        return topology;
    }
}

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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Ignore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author chenghui.lch
 */
@Ignore
public class PartitionTestBase extends DDLBaseNewDBTestCase {

    protected static final Log log = LogFactory.getLog(BaseTestCase.class);

    public boolean usingNewPartDb() {
        return true;
    }

    protected Long getTableGroupId(String tableName, boolean likeTableName) {
        if (likeTableName) {
            tableName = "%" + tableName + "%";
            String query = String.format(
                "select group_id from table_partitions where table_schema='%s' and table_name like '%s' and part_level='0'",
                tddlDatabase1, tableName);
            return queryTableGroup(query);
        } else {
            return getTableGroupId(tableName);
        }

    }

    protected Long getTableGroupId(String tableName) {
        String query = String.format(
            "select group_id from table_partitions where table_schema='%s' and table_name='%s' and part_level='0'",
            tddlDatabase1, tableName);
        return queryTableGroup(query);
    }

    private Long queryTableGroup(String query) {
        Long tableGroupId = -1L;
        Connection metaConnection = getMetaConnection();
        ResultSet rs = JdbcUtil.executeQuery(query, metaConnection);
        try {
            if (rs.next()) {
                tableGroupId = rs.getLong("group_id");
            } else {
                Assert.fail("can't find the table meta");
            }
        } catch (SQLException ex) {
            log.error(ex);
            Assert.fail(ex.getMessage());
        }
        Assert.assertTrue(tableGroupId.longValue() != -1);
        return tableGroupId;
    }
}


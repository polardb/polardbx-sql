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

package com.alibaba.polardbx.qatest.ddl.datamigration.balancer;

import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Create partition-table database in test
 *
 * @author moyi
 * @since 2021/06
 */
@NotThreadSafe
public abstract class BalancerTestBase extends LocalityTestBase {

    protected static final Log log = LogFactory.getLog(BalancerTestBase.class);

    protected static String logicalDBName = "BalancerTestBase";

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + logicalDBName);
            JdbcUtil.executeUpdateSuccess(tmpConnection,
                "create database " + logicalDBName + " mode='auto'");
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + logicalDBName);
        }
    }

    @Before
    public void beforeBaseAutoPartitionNewPartition() {
        JdbcUtil.useDb(tddlConnection, logicalDBName);
    }

}

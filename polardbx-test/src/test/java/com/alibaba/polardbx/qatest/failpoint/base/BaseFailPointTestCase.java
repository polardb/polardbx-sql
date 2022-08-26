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

package com.alibaba.polardbx.qatest.failpoint.base;

import com.alibaba.polardbx.qatest.ddl.auto.dag.BaseDdlEngineTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class BaseFailPointTestCase extends BaseDdlEngineTestCase {

    protected static final String FAIL_POINT_SCHEMA_NAME = "fail_point";

    protected Connection failPointConnection;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + FAIL_POINT_SCHEMA_NAME);
            JdbcUtil.executeUpdateSuccess(tmpConnection, "create database " + FAIL_POINT_SCHEMA_NAME);
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.executeUpdateSuccess(tmpConnection, "drop database if exists " + FAIL_POINT_SCHEMA_NAME);
        }
    }

    @Before
    public void beforeBaseFailPointTestCase() {
        useDb(tddlConnection, FAIL_POINT_SCHEMA_NAME);
        this.failPointConnection = tddlConnection;
    }
}

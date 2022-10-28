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

package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

public class ProcedureLongTextTest extends BaseTestCase {
    protected Connection tddlConnection;

    private String procedureName = "procedure_long_text_test";

    private String createProcedure = "CREATE PROCEDURE %s() comment '%s' begin %s end;";

    private String callProcedure = "call %s()";

    private String dropProcedure = "DROP PROCEDURE IF EXISTS %s";

    @Before
    public void getConnection() {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, "set @@max_allowed_packet = " + 102428800);
    }

    @Test
    public void testLongDefinition() {
        String comment = "test";
        String body = StringUtils.repeat("set @x = 1;", 1024 * 100);
        JdbcUtil.executeSuccess(tddlConnection, String.format(createProcedure, procedureName, comment, body));
        JdbcUtil.executeSuccess(tddlConnection, String.format(callProcedure, procedureName));
    }

    @Test
    public void testLongComment() {
        String comment = StringUtils.repeat("procedure_test_", 1024 * 100);
        String body = "set @x = 1;";
        JdbcUtil.executeSuccess(tddlConnection, String.format(createProcedure, procedureName, comment, body));
        JdbcUtil.executeSuccess(tddlConnection, String.format(callProcedure, procedureName));
    }

    @After
    public void dropProcedure() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropProcedure, procedureName));
    }
}

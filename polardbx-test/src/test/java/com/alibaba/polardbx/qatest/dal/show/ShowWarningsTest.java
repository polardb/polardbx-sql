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

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Test suite for SHOW WARNINGS
 *
 * @author Eric Fu
 */
public class ShowWarningsTest extends ReadBaseTestCase {

    @Test
    public void testSelectWithBadHint() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("/*+TDDL:SOME_BAD_HINT=42*/ select now()")) {
                rs.next();
            }

            try (ResultSet rs = stmt.executeQuery("show warnings")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("Error", rs.getString("Level"));
                Assert.assertTrue(rs.getString("Message").contains("Illegal hint"));
            }
        }
    }
}

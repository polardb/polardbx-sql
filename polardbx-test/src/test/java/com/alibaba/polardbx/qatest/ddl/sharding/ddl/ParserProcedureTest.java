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

package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class ParserProcedureTest extends AsyncDDLBaseNewDBTestCase {
    @Parameterized.Parameters(name = "{index}:{0},{1},{2}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(
            deterministic(),
            sqlType(),
            security());
    }

    public static List<Object[]> cartesianProduct(Object[]... arrays) {
        List[] lists = Arrays.stream(arrays)
            .map(Arrays::asList)
            .toArray(List[]::new);
        List<List<Object>> result = Lists.cartesianProduct(lists);
        return result.stream()
            .map(List::toArray)
            .collect(Collectors.toList());
    }

    public static Object[] deterministic() {
        return new Object[] {
            Boolean.TRUE,
            Boolean.FALSE
        };
    }

    public static Object[] sqlType() {
        return new Object[] {
            "",
            "CONTAINS SQL",
            "NO SQL",
            "READS SQL DATA",
            "MODIFIES SQL DATA"
        };
    }

    public static Object[] security() {
        return new Object[] {
            "DEFINER",
            "INVOKER"
        };
    }

    static String CREATE_PROCEDURE_TEMPLATE = "CREATE DEFINER = 'root'@'127.0.0.1' PROCEDURE %s\n"
        + " (IN val1 int , INOUT val2 double, OUT val3 text ) \n"
        + "COMMENT 'test`aaa`\"xx\"\"dsagsag\"' LANGUAGE SQL %s DETERMINISTIC %s \n"
        + "SQL SECURITY %s \n"
        + "BEGIN\n"
        + "SELECT 1;\n"
        + "END; ";

    static String DROP_PROCEDURE_TEMPLATE = "DROP PROCEDURE IF EXITS %s";

    private final String name;
    private final String isDeterminstic;
    private final String sqlType;
    private final String security;

    public ParserProcedureTest(Object isDeterminstic, Object sqlType, Object security) {
        this.isDeterminstic = (Boolean) isDeterminstic ? "" : "NOT";
        this.sqlType = sqlType.toString();
        this.security = security.toString();
        this.name = RandomStringUtils.randomAlphabetic(10);
    }

    @Test
    public void testParseAndCreate() {
        String createProcedure = String.format(CREATE_PROCEDURE_TEMPLATE, name, isDeterminstic, sqlType, security);
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(createProcedure);
        } catch (SQLException ex) {
            Assert.fail("create failed, sql: " + createProcedure);
        }
    }

    @Before
    @After
    public void cleanProcedure() {
        dropProcedureIfExists();
    }

    public void dropProcedureIfExists() {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute(String.format(DROP_PROCEDURE_TEMPLATE, name));
        } catch (SQLException ex) {

        }
    }
}

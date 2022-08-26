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

package com.alibaba.polardbx.qatest.cdc;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Created by ziyang.lb
 */
public class PrepareData {

    private static final String CREATE_TABLE =
        "CREATE TABLE if not exists accounts "
            + "(id INT PRIMARY KEY, "
            + "balance INT NOT NULL) "
            + "ENGINE=InnoDB  DBPARTITION BY HASH(id)";

    private static final String PREPARE_DATA_FORMAT = "INSERT INTO accounts VALUES %s";

    private static void cleanTable(Connection connection) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("truncate table accounts");
        ps.executeUpdate();
    }

    private static void createTable(Connection connection) throws SQLException {
        PreparedStatement ps = connection.prepareStatement(CREATE_TABLE);
        ps.executeUpdate();
    }

    private static void prepareData(Connection connection, int accountCount, int initialBalance) throws SQLException {
        List<String> valueList = Lists.newArrayListWithCapacity(accountCount);
        for (int i = 0; i < accountCount; i++) {
            valueList.add(String.format("(%d, %d)", i, initialBalance));
        }
        PreparedStatement ps = connection.prepareStatement(String.format(PREPARE_DATA_FORMAT,
            StringUtils.join(valueList, ",")));
        ps.executeUpdate();
    }

    public static void init(Connection connection, int accountCount, int initialBalance) throws SQLException {
        createTable(connection);
        cleanTable(connection);
        prepareData(connection, accountCount, initialBalance);
    }

}

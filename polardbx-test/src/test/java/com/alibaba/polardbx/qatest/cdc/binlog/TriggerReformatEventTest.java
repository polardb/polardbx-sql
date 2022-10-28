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

package com.alibaba.polardbx.qatest.cdc.binlog;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.cdc.CdcCheckTest;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 触发整形测试
 * created by ziyang.lb
 **/
public class TriggerReformatEventTest extends CdcCheckTest {

    private static final String DDL =
        "create table if not exists multi_rows_update("
            + "`name` varchar(20)  COLLATE utf8mb4_bin DEFAULT NULL, "
            + "a_score_1 int DEFAULT NULL,"
            + "a_score_2 int DEFAULT 1,"
            + "age int DEFAULT 1 ,"
            + " message varchar(200)  COLLATE utf8mb4_bin , "
            + "extra text  COLLATE utf8mb4_bin , "
            + "`update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_bin";

    private static final String KEY_NAME = "atest";

    private void prepareTable() throws SQLException {
        Connection connection = srcDs.getConnection();
        try{
            Statement st = connection.createStatement();
            st.executeUpdate(DDL);
            st.close();
        }finally {
            connection.close();
        }
    }

    private void prepare() throws SQLException {
        Random r = new Random();
        Connection connection = srcDs.getConnection();
        try{
            Statement st = connection.createStatement();
            st.executeUpdate("truncate table multi_rows_update");

            for (int i=0 ; i<r.nextInt(20) + 2; i ++){
                String sql = String.format("insert into multi_rows_update(`name`, age, message, extra) values('%s', %s, '%s', '%s')",
                    KEY_NAME, r.nextInt(10)+"", RandomStringUtils.random(100), RandomStringUtils.random(100));
                st.executeUpdate(sql);
            }
            st.close();
        }finally {
            connection.close();
        }

    }

    private void reformatUpdate() throws SQLException {
        Connection connection = srcDs.getConnection();
        try{
            Statement st = connection.createStatement();
            String sql = String.format("update multi_rows_update set extra = '%s' where name = '%s'", RandomStringUtils.random(100), KEY_NAME);

            st.executeUpdate(sql);
        }finally {
            connection.close();
        }
    }

    private void reformatDelete() throws SQLException {
        Connection connection = srcDs.getConnection();
        try{
            Statement st = connection.createStatement();
            String sql = String.format("delete from multi_rows_update  where name = '%s'", KEY_NAME);
            st.executeUpdate(sql);
        }finally {
            connection.close();
        }
    }

    @Test
    public void testReformat() throws Exception {
        prepareTable();
        String dbName = PropertiesUtil.polardbXDBName1(false);
        safePoint();
        prepare();
        safePoint();
        checkOneTable(new Pair<>(dbName, "multi_rows_update"));
        reformatUpdate();
        safePoint();
        checkOneTable(new Pair<>(dbName, "multi_rows_update"));
        reformatDelete();
        safePoint();
        checkOneTable(new Pair<>(dbName, "multi_rows_update"));
    }

}

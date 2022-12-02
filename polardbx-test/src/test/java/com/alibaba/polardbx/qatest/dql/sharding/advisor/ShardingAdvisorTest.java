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

package com.alibaba.polardbx.qatest.dql.sharding.advisor;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertWithMessage;

/**
 * @author Shi Yuxuan
 */

public class ShardingAdvisorTest extends BaseTestCase {

    private String db;
    private String mode;
    private final String RESOURCES_PATH = "dql/sharding/advisor/sharding.%s.%s.yml";
    private Map<String, String> tables;
    Connection conn;

    private static String DRDS_DB = "sharding_advisor_partition";
    private static String CREATE_DRDS_DB = "CREATE DATABASE " + DRDS_DB + " mode = 'DRDS'";
    private static String DROP_DRDS_DB = "DROP DATABASE IF EXISTS " + DRDS_DB;

    private static String AUTO_DB = "sharding_advisor_auto";
    private static String CREATE_AUTO_DB = "CREATE DATABASE " + AUTO_DB + " mode = 'AUTO'";
    private static String DROP_AUTO_DB = "DROP DATABASE IF EXISTS " + AUTO_DB;

    private static String AUTOGSI_DB = "sharding_advisor_autogsi";
    private static String CREATE_AUTOGSI_DB = "CREATE DATABASE " + AUTOGSI_DB + " mode = 'AUTO'";
    private static String DROP_AUTOGSI_DB = "DROP DATABASE IF EXISTS " + AUTOGSI_DB;


    public ShardingAdvisorTest(String db, String mode) {
        this.db = db;
        this.mode = mode;
    }

    @BeforeClass
    public static void initDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(DROP_DRDS_DB);
            stmt.execute(CREATE_DRDS_DB);

            stmt.execute(DROP_AUTO_DB);
            stmt.execute(CREATE_AUTO_DB);


            stmt.execute(DROP_AUTOGSI_DB);
            stmt.execute(CREATE_AUTOGSI_DB);
        }
    }

    @AfterClass
    public static void deleteDb() throws SQLException {
        try (Connection tmpConnection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            Statement stmt = tmpConnection.createStatement()) {
            stmt.execute(DROP_DRDS_DB);
            stmt.execute(DROP_AUTO_DB);
            stmt.execute(DROP_AUTOGSI_DB);
        }
    }

    @Before
    public void before() {
        this.conn = getPolardbxConnection(db);
        Yaml yaml = new Yaml();
        tables = yaml.load(readToString(String.format(RESOURCES_PATH, mode, "ddl")));

        // create tables
        try (final Statement stmt = conn.createStatement()) {
            for (String ddl : tables.values()) {
                stmt.execute(ddl);
            }

            String dmls = readToString(String.format(RESOURCES_PATH, mode, "dml"));
            if (dmls != null) {
                for (String dml : (List<String>)yaml.load(dmls)) {
                    stmt.execute(dml);
                }
            }
            for (String table : tables.keySet()) {
                stmt.execute("analyze table " + table);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @After
    public void after() {
        // drop table
        try (final Statement stmt = conn.createStatement()) {
            if (tables != null) {
                for (String table : tables.keySet()) {
                    stmt.execute("drop table if exists " + table);
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(new Object[]{DRDS_DB, "drds"}, new Object[]{AUTO_DB, "auto"}, new Object[]{AUTOGSI_DB, "autogsi"});
    }

    @Test
    public void shardingAdvise() {
        Yaml yaml = new Yaml();
        List<String> sqls = yaml.load(readToString(String.format(RESOURCES_PATH, mode, "sql")));
        for (String sql : sqls) {
            JdbcUtil.executeQuery(sql, conn);
        }
        try (PreparedStatement ps = JdbcUtil.preparedStatement(
            "/*+TDDL:cmd_extra(SHARDING_ADVISOR_BROADCAST_THRESHOLD=-1, SHARDING_ADVISOR_RECODE_PLAN=true)*/shardingadvise",
            conn)) {
            ResultSet rs = ps.executeQuery();
            List<List<Object>> rsList = JdbcUtil.getAllResult(rs, false);
            assertWithMessage("sharding advisor fail!").that(rsList.size())
                .isEqualTo(1);

            List<String> results = yaml.load(readToString(String.format(RESOURCES_PATH, mode, "result")));
            String plan = (String) rsList.get(0).get(1);
            for (String result : results) {
                assertWithMessage("different sharding plan").that(plan).contains(result);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private String readToString(String fileName) {
        String path = ConfigConstant.class.getClassLoader().getResource(".").getPath() + fileName;
        File file = new File(path);
        if (!file.exists()) {
            return null;
        }
        try (InputStream in = new FileInputStream(path)) {
            return IOUtils.toString(in, StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return null;
    }
}

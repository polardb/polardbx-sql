/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class HaTest extends ReadBaseTestCase {
    @Ignore("manual test only")
    @Test
    public void testHa() throws Exception {
        String target = null;
        try (final ResultSet rs = JdbcUtil.executeQuery(
            "/*+TDDL: node(1)*/select * from information_schema.alisql_cluster_global", tddlConnection)) {
            while (rs.next()) {
                if (rs.getString("ROLE").equalsIgnoreCase("Leader")) {
                    System.out.println("leader is: " + rs.getString("IP_PORT"));
                    continue;
                }
                if (rs.getString("ELECTION_WEIGHT").equalsIgnoreCase("1")) {
                    continue;
                }
                final String[] address = rs.getString("IP_PORT").split(":");
                if (address.length == 2) {
                    target = address[0] + ':' + (Integer.parseInt(address[1]) - 8000);
                }
            }
        }
        if (target != null) {
            System.out.println("target is: " + target);

            // start concurrent inserts first
            JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists ha_test");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "create table ha_test(pk int primary key auto_increment, a int) dbpartition by hash(pk)");

            final AtomicBoolean exit = new AtomicBoolean(false);
            final Thread th = new Thread(() -> {
                while (!exit.get()) {
                    try (final Connection conn = getPolardbxConnection()) {
                        JdbcUtil.executeUpdateSuccess(conn, "insert into ha_test (a) values(1)");
                    } catch (Throwable t) {
                        System.out.println(t.getMessage());
                    }
                }
            });
            th.start();

            Thread.sleep(1000);

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "/*+TDDL:cmd_extra(FORCE_CHANGE_ROLE=true)*/ALTER SYSTEM CHANGE_ROLE NODE '" + target + "' TO LEADER");

            // wait until switch done
            final long startMs = System.currentTimeMillis();
            boolean got = false;
            while (System.currentTimeMillis() - startMs < 60000) {
                try (final ResultSet rs = JdbcUtil.executeQuery("show datasources", tddlConnection)) {
                    while (rs.next()) {
                        final String name = rs.getString("NAME");
                        if (name.contains(target.replace(':', '-'))) {
                            got = true;
                            break;
                        }
                    }
                }
                Thread.sleep(1000);
            }
            Assert.assertTrue(got);

            // stop concurrent inserts
            exit.set(true);
            th.join();
        }
    }
}

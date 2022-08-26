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

package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.BaseSequenceTestCase.quoteSpecialName;

/**
 * @version 1.0
 */
public class LargeBatchDml extends ReadBaseTestCase {

    private static final String TABLE0_NAME = "xproto_large0";
    private static final String TABLE0 = "CREATE TABLE `" + TABLE0_NAME + "` (\n"
        + "        `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "        `agency_company_code` varchar(6) DEFAULT '',\n"
        + "        `bill_code` varchar(50) NOT NULL DEFAULT '',\n"
        + "        `bill_code_bak` varchar(255) DEFAULT '',\n"
        + "        PRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 dbpartition by HASH(`id`)";
    private static final String TABLE0_PART = "CREATE TABLE `" + TABLE0_NAME + "` (\n"
        + "        `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "        `agency_company_code` varchar(6) DEFAULT '',\n"
        + "        `bill_code` varchar(50) NOT NULL DEFAULT '',\n"
        + "        `bill_code_bak` varchar(255) DEFAULT '',\n"
        + "        PRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 partition by KEY(`id`) partitions 3";

    private static final String TABLE1_NAME = "xproto_large1";
    private static final String TABLE1 = "CREATE TABLE `" + TABLE1_NAME + "` (\n"
        + "        `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "        `agency_company_code` varchar(6) DEFAULT '',\n"
        + "        `bill_code` varchar(16) NOT NULL DEFAULT '',\n"
        + "        `bill_code_bak` varchar(255) DEFAULT '',\n"
        + "        PRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 dbpartition by HASH(`id`)";

    private static final String TABLE1_PART = "CREATE TABLE `" + TABLE1_NAME + "` (\n"
        + "        `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "        `agency_company_code` varchar(6) DEFAULT '',\n"
        + "        `bill_code` varchar(16) NOT NULL DEFAULT '',\n"
        + "        `bill_code_bak` varchar(255) DEFAULT '',\n"
        + "        PRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 partition by KEY(`id`) partitions 3";

    @Before
    public void before() {
        if (usingNewPartDb()) {
            return;
        }
        cleanup();

        JdbcUtil.executeUpdateSuccess(tddlConnection, usingNewPartDb() ? TABLE0_PART : TABLE0);
        JdbcUtil.executeUpdateSuccess(tddlConnection, usingNewPartDb() ? TABLE1_PART : TABLE1);

        for (int i = 0; i < 5; ++i) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "insert into " + TABLE0_NAME + " values (null, '000004', 'bill', 'bak')");
        }

        for (int i = 0; i < 100; ++i) {
            final StringBuilder builder = new StringBuilder();
            builder.append("insert into ").append(TABLE1_NAME).append(" values ");
            for (int j = 0; j < 100; ++j) {
                if (j != 0) {
                    builder.append(", ");
                }
                builder.append("(null, '000004', 'bill', 'bak')");
            }
            JdbcUtil.executeUpdateSuccess(tddlConnection, builder.toString());
        }
    }

    @After
    public void cleanup() {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE0_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE1_NAME));
    }

    @Test
    public void testLargeBatchDml() {
        if (usingNewPartDb()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "UPDATE `" + TABLE1_NAME + "` a LEFT JOIN `" + TABLE0_NAME
                + "` b ON a.bill_code_bak=b.bill_code_bak AND a.agency_company_code=b.agency_company_code SET a.bill_code=b.bill_code where a.agency_company_code ='000004'");
    }

}

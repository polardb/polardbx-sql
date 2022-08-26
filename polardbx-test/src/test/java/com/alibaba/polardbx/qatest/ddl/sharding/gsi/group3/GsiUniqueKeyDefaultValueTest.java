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

package com.alibaba.polardbx.qatest.ddl.sharding.gsi.group3;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;

/**
 * @author chenmo.cm
 */
public class GsiUniqueKeyDefaultValueTest extends DDLBaseNewDBTestCase {

    private static final String PRIMARY_NAME = "uk_default_test_primary";
    private static final String GSI_NAME = "g_i_uk_default";

    private static final String HINT =
        "/*+TDDL:CMD_EXTRA(GSI_DEFAULT_CURRENT_TIMESTAMP=TRUE, GSI_ON_UPDATE_CURRENT_TIMESTAMP=TRUE)*/";
    private static final String CREATE_TMPL = HINT + "CREATE TABLE `" + PRIMARY_NAME + "` ("
        + " `pk` int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1` int(11) NOT NULL,"
        + " `c2` int(11) NOT NULL DEFAULT 0,"
        + " `c3` varchar(128) DEFAULT NULL,"
        + " {0},"
        + " `pad` varchar(256) DEFAULT NULL,"
        + " {1},"
        + " PRIMARY KEY (`pk`)"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8 {2}";

    @Before
    public void before() {

        // Drop table
        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_NAME);
//      // JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + PRIMARY_NAME);
        dropTableWithGsi(PRIMARY_NAME, ImmutableList.of(GSI_NAME));
    }

    /**
     * NOT NULL 但是没有 DEFAULT VALUE，校验 INSERT 报错信息
     */
    @Test
    public void testErrorForColumnWithoutDefault() {
        final String createTableWithoutGsi = MessageFormat
            .format(CREATE_TMPL, "`c4` varchar(128) not null", "key `" + GSI_NAME + "`(c4)", "dbpartition by hash(pk)");

        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableWithoutGsi);

        JdbcUtil.executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1, c4) VALUES(DEFAULT, 'a')",
            "Column 'c1' has no default value");

        JdbcUtil.executeUpdateFailed(tddlConnection, "INSERT INTO " + PRIMARY_NAME + "(c1) VALUES(DEFAULT)",
            "Column 'c1' has no default value");
    }
}

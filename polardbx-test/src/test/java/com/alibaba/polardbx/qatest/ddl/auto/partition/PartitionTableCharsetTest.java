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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.bouncycastle.pqc.math.linearalgebra.ByteUtils;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author chenghui.lch
 */
@Ignore
public class PartitionTableCharsetTest extends PartitionTestBase {

    @Test
    public void testInsertGbk() {
        try {
            String useDb = "use testmysql";

            JdbcUtil.executeUpdate(mysqlConnection, useDb);

            String charSet = "gbk";
            String setNames = "set names " + charSet;
            JdbcUtil.executeUpdate(mysqlConnection, setNames);

            String p0Bnd = "世界";
            String p1Bnd = "世界人民";
            String p2Bnd = "世界人民万岁";

            String p0BndHexStr = ByteUtils.toHexString(p0Bnd.getBytes(charSet));
            String p1BndHexStr = ByteUtils.toHexString(p1Bnd.getBytes(charSet));
            String p2BndHexStr = ByteUtils.toHexString(p2Bnd.getBytes(charSet));

            String insertSql = String
                .format("insert into test_gbk values (null, x'%s'),(null, x'%s'),(null, x'%s')", p0BndHexStr,
                    p1BndHexStr, p2BndHexStr);
            JdbcUtil.executeUpdate(mysqlConnection, insertSql);

            //dbcUtil.executeUpdate(mysqlConnection, insertSql);

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    @Test
    public void testCreateTableWithSetNamesForRange() {

        String tblName = "rng_col_str_const";
        try {

            String dropTbIfExists = "drop table if exists " + tblName;

            String charSet = "gbk";
            String setNames = "set names " + charSet;

            String p0Bnd = "世界";
            String p1Bnd = "世界人民";
            String p2Bnd = "世界人民万岁";

            String ddlSql = String.format("CREATE TABLE `rng_col_str_const` (\n"
                + "  `a` int(11) NOT NULL AUTO_INCREMENT,\n"
                + "  `b` varchar(256) NOT NULL,\n"
                + "  PRIMARY KEY (`a`,`b`)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=gbk\n"
                + "PARTITION BY RANGE COLUMNS(b)\n"
                + "(PARTITION p0 VALUES LESS THAN (x\"CAC0BDE7\") ENGINE = InnoDB,\n"
                + " PARTITION p1 VALUES LESS THAN (x\"b%s\") ENGINE = InnoDB,\n"
                + " PARTITION p2 VALUES LESS THAN (\"c%s\") ENGINE = InnoDB);", p0Bnd, p1Bnd, p2Bnd);

            String showTables = "show tables";

            JdbcUtil.executeUpdate(tddlConnection, dropTbIfExists);
            JdbcUtil.executeUpdate(tddlConnection, setNames);
            JdbcUtil.executeUpdate(tddlConnection, ddlSql);
            JdbcUtil.executeUpdate(tddlConnection, showTables);

        } catch (Throwable ex) {
            ex.printStackTrace();
        }

    }
}

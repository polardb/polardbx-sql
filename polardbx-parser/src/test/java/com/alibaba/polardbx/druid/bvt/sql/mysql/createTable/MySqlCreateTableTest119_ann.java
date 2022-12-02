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

package com.alibaba.polardbx.druid.bvt.sql.mysql.createTable;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlCreateTableTest119_ann extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE TABLE face_feature (\n" +
            "  id varchar COMMENT 'id',\n" +
            "  facefea array<short>(256) COMMENT 'feature',\n" +
            "  ANN INDEX facefea_index1 (facefea) DistanceMeasure = DotProduct ALGORITHM = IVF\n" +
            "  PRIMARY KEY (id)\n" +
            ")\n" +
            "PARTITION BY HASH KEY (id) PARTITION NUM 8\n" +
            "TABLEGROUP vector_demo_group\n" +
            "OPTIONS (UPDATETYPE='batch')\n" +
            "COMMENT '';";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLCreateTableStatement stmt = (SQLCreateTableStatement) statementList.get(0);

        assertEquals(1, statementList.size());
        assertEquals(4, stmt.getTableElementList().size());

        assertEquals("CREATE TABLE face_feature (\n" +
            "\tid varchar COMMENT 'id',\n" +
            "\tfacefea ARRAY<short>(256) COMMENT 'feature',\n" +
            "\tINDEX facefea_index1 ANN(facefea) ALGORITHM = IVF DistanceMeasure = DotProduct,\n" +
            "\tPRIMARY KEY (id)\n" +
            ")\n" +
            "OPTIONS (UPDATETYPE = 'batch') COMMENT ''\n" +
            "PARTITION BY HASH KEY(id) PARTITION NUM 8\n" +
            "TABLEGROUP = vector_demo_group;", stmt.toString());

        MySqlTableIndex idx = (MySqlTableIndex) stmt.findIndex("facefea");
        assertNotNull(idx);

        assertEquals("DotProduct", idx.getDistanceMeasure());
        assertEquals("IVF", idx.getAlgorithm());
        assertEquals("ANN", idx.getIndexType());
    }
}
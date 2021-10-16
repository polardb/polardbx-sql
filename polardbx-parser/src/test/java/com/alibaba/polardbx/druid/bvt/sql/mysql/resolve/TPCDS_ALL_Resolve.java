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

package com.alibaba.polardbx.druid.bvt.sql.mysql.resolve;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.benchmark.TPCDS;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDataType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import junit.framework.TestCase;

import java.util.List;

public class TPCDS_ALL_Resolve extends TestCase {
    private SchemaRepository repository = new SchemaRepository(DbType.mysql);

    protected void setUp() throws Exception {
        repository.acceptDDL(TPCDS.getDDL());
    }

    public void test_q01() throws Exception {
        for (int q = 1; q <= 99; ++q) {
            System.out.println("tpcds query-" + q);
            System.out.println("-----------------------------------------------------");
            String sql = TPCDS.getQuery(q);

            final List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql);

            for (SQLStatement stmt : statements) {
                repository.resolve(stmt);

                final SQLSelect select = ((SQLSelectStatement) stmt).getSelect();
                final SQLSelectQueryBlock firstQueryBlock = select.getFirstQueryBlock();
                if (firstQueryBlock == null) {
                    continue;
                }

                final List<SQLSelectItem> selectList = firstQueryBlock.getSelectList();
                for (int i = 0; i < selectList.size(); i++) {
                    SQLSelectItem selectItem = selectList.get(i);
                    if (selectItem.getExpr() instanceof SQLAllColumnExpr) {
                        continue;
                    }
                    final SQLDataType dataType = selectItem.computeDataType();
                    if (dataType == null) {
//                        fail("dataType is null : " + selectItem);
                    }
                }
            }
        }
    }

}

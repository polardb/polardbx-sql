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

package com.alibaba.polardbx.optimizer;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.junit.Before;

import java.sql.SQLSyntaxErrorException;

public class BaseRuleTest extends BasePlannerTest {

    protected String XX =
        "{\"rels\":[{\"id\":\"0\",\"relOp\":\"LogicalView\",\"table\":[\"optest\",\"emp\"],\"tableNames\":[\"emp\"],\"pushDownOpt\":{\"pushrels\":[{\"id\":\"0\",\"relOp\":\"LogicalTableScan\",\"table\":[\"optest\",\"emp\"],\"flashback\":null,\"inputs\":[]}]},\"schemaName\":\"optest\",\"partitions\":[],\"flashback\":null},{\"id\":\"1\",\"relOp\":\"LogicalFilter\",\"condition\":{\"op\":\"SqlBinaryOperator=\",\"operands\":[{\"input\":2,\"name\":\"$2\",\"type\":{\"type\":\"TINYINT\",\"nullable\":true,\"precision\":1}},10],\"type\":{\"type\":\"BIGINT\",\"nullable\":true}}},{\"id\":\"2\",\"relOp\":\"HashAgg\",\"group\":[0],\"aggs\":[{\"agg\":\"SqlCountAggFunctionCOUNT\",\"type\":{\"type\":\"BIGINT\",\"nullable\":true},\"distinct\":true,\"operands\":[1],\"filter\":-1},{\"agg\":\"SqlCountAggFunctionCOUNT\",\"type\":{\"type\":\"BIGINT\",\"nullable\":true},\"distinct\":false,\"operands\":[],\"filter\":-1}]}]}";

    public static String empDDL =
        "CREATE TABLE emp(\n" + "  userId int, \n" + "  name varchar(30), \n" + "  operation tinyint(1), \n"
            + "  actionDate varchar(30)\n"
            + "  ) dbpartition by hash(userId) tbpartition by HASH(actionDate) tbpartitions 7;";

    public static String stuDDL =
        "CREATE TABLE stu(\n" + "  id int, \n"
            + "  name varchar(30), \n"
            + "  operation tinyint(1), \n"
            + "  actionDate varchar(30),\n"
            + "  primary(id)\n"
            + "  ) dbpartition by hash(id) tbpartition by HASH(actionDate) tbpartitions 7;";
    protected RelOptCluster relOptCluster;
    protected RelOptSchema schema;

    public static final String SCHEMA_NAME = "optest";

    public BaseRuleTest() {
        super(SCHEMA_NAME);
    }

    @Before
    public void init() {
        relOptCluster = SqlConverter.getInstance(appName, new ExecutionContext()).createRelOptCluster();
        schema = SqlConverter.getInstance(appName, new ExecutionContext()).getCatalog();
        try {
            loadStatistic();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            buildTable(SCHEMA_NAME, empDDL);
            buildTable(SCHEMA_NAME, stuDDL);
        } catch (SQLSyntaxErrorException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }
}

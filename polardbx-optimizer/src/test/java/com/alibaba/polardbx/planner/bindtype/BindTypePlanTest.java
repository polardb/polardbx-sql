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

package com.alibaba.polardbx.planner.bindtype;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.sql.repository.SchemaResolveVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import org.apache.calcite.sql.SqlNode;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(EclipseParameterized.class)
public class BindTypePlanTest extends BasePlannerTest {

    public BindTypePlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(BindTypePlanTest.class);
    }

    @Override
    protected String getPlan(String testSql) {
        Map<Integer, ParameterContext> currentParameter = new HashMap<>();
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setServerVariables(new HashMap<>());
        executionContext.setAppName(appName);
        executionContext.setParams(new Parameters());
        SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(
            ByteString.from(testSql), currentParameter, executionContext, true);

        Planner.processParameters(sqlParameterized.getParameters(), executionContext);

        List<SQLStatement> stmtList = FastsqlUtils.parseSql(sqlParameterized.getSql());
        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);

        repository.resolve(stmtList.get(0), SchemaResolveVisitor.Option.ResolveAllColumn,
            SchemaResolveVisitor.Option.ResolveIdentifierAlias);

        ContextParameters parserParameters = new ContextParameters(false);
        parserParameters.setParameterNlsStrings(executionContext.getParameterNlsStrings());

        SqlNode converted = FastsqlParser.convertStatementToSqlNode(
            stmtList.get(0), sqlParameterized.getParameters(), parserParameters, ec);
        return converted.toString();
    }
}


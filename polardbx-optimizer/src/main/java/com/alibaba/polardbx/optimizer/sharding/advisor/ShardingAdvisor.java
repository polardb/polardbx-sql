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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelMetadataProvider;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.hint.operator.HintCmdOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.index.TableScanFinder;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is the entry class of sharding advisor.
 *
 * @author shengyu
 */
public class ShardingAdvisor {
    private static final Logger logger = LoggerFactory.getLogger(ShardingAdvisor.class);
    ShardColumnRelFinder shardColumnRelFinder;

    private ParamManager paramManager;

    public ShardingAdvisor() {
        paramManager = new ParamManager(new HashMap<>());
        shardColumnRelFinder = new ShardColumnRelFinder(paramManager);
    }

    public ShardingAdvisor(ByteString sql) {
        try {
            HashMap<String, Object> cmdObjects = new HashMap<>();
            int index = sql.toString().toLowerCase().indexOf("shardingadvise");
            List<SQLStatement> stmtList = FastsqlUtils.parseSql(sql.substring(0, index));
            ContextParameters contextParameters = new ContextParameters(false);
            for (SQLStatement statement : stmtList) {
                List<SQLCommentHint> hintList = ((MySqlHintStatement) statement).getHints();
                SqlNodeList sqlNodes =
                    FastSqlConstructUtils.convertHints(hintList, contextParameters, new ExecutionContext());
                HintConverter.HintCollection collection = new HintConverter.HintCollection();
                HintUtil.collectHint(sqlNodes, collection, false, new ExecutionContext());
                List<HintCmdOperator> hintCmdOperators = collection.cmdHintResult;
                HintCmdOperator.CmdBean cmdBean = new HintCmdOperator.CmdBean("", cmdObjects, "");
                for (HintCmdOperator op : hintCmdOperators) {
                    op.handle(cmdBean);
                }
            }
            paramManager = new ParamManager(cmdObjects);
            shardColumnRelFinder = new ShardColumnRelFinder(paramManager);
        } catch (Exception e) {
            logger.error("parser the sharding advise hint", e);
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * visit the ast to find equivalent column set
     *
     * @param relNode the node to be visited
     * @param count the occurrence of ast
     */
    public void addPlan(RelNode relNode, int count) {
        shardColumnRelFinder.setMq(RelMetadataQuery.instance());
        shardColumnRelFinder.setCount(count);
        shardColumnRelFinder.go(relNode);
        shardColumnRelFinder.getShardColumnEdges().addEdges();
    }

    public ParamManager getParamManager() {
        return paramManager;
    }

    /**
     * the place we actually search for a sharding plan
     *
     * @return a sharding plan
     */
    private ShardResultForOutput adviseAfterBuild() {
        // build the graph
        JoinGraphCut jgc = new JoinGraphCut(paramManager);
        for (ShardColumnEdges.EdgeDetail edge : shardColumnRelFinder.getShardColumnEdges().toList()) {
            jgc.addEdge(edge.name1, edge.col1, edge.name2, edge.col2, edge.weight);
        }
        // deal with broadcast table
        jgc.dealWithBroadCast(shardColumnRelFinder.getShardColumnEdges().getRowCounts());
        // search for a sharding plan
        jgc.analyse();

        return new ShardResultForOutput(shardColumnRelFinder.getShardColumnEdges(), jgc.getBests());
    }

    /**
     * advise from plan cache
     * while the whole process is capable of advising multi schema, sqls related to schemas other than schemaName
     * are filtered for speed.
     *
     * @param schemaName the schema to be tested
     * @param caches plan caches fetched from all cn nodes
     * @return the result of sharding advise
     */
    public ShardResultForOutput adviseFromPlanCache(String schemaName, Map<String, AdvisorCache> caches) {
        RelMetadataQuery.THREAD_PROVIDERS.set(JaninoRelMetadataProvider.of(DrdsRelMetadataProvider.INSTANCE));
        // record the sql needed
        List<Pair<SqlParameterized, Integer>> sqls = new ArrayList<>();
        //consider plan cache
        for (AdvisorCache cache : caches.values()) {
            if (cache.getParameters() == null) {
                continue;
            }
            // build parameters
            ExecutionContext ec = new ExecutionContext();
            ec.setSchemaName(schemaName);
            Parameters para = new Parameters();
            para.setParams(OptimizerUtils.buildParam(cache.getParameters()));
            ec.setParams(para);
            ec.setServerVariables(new HashMap<>());
            PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);

            // optimize the plan
            ExecutionPlan executionPlan;
            try {
                executionPlan = getPlan(plannerContext,
                    new SqlParameterized(cache.getSql(), cache.getParameters()), schemaName);
            } catch (RuntimeException e) {
                continue;
            }
            if (executionPlan == null) {
                continue;
            }

            addPlan(executionPlan.getPlan(), (int) cache.getHitCount());
            sqls.add(new Pair<>(
                new SqlParameterized(cache.getSql(), cache.getParameters()),
                (int) cache.getHitCount()));
        }

        ShardResultForOutput shardResultForOutput = adviseAfterBuild();
        shardResultForOutput.setSqls(sqls);

        return shardResultForOutput;
    }

    private ExecutionPlan getPlan(PlannerContext pc, SqlParameterized sqlParameterized, String schemaName) {
        if (sqlParameterized.getSql().toLowerCase().contains("information_schema")) {
            return null;
        }
        Planner planner = new RewriterPlanner();

        SqlNodeList astList = new FastsqlParser()
            .parse(sqlParameterized.getSql(), sqlParameterized.getParameters(), pc.getExecutionContext());
        SqlNode ast = astList.get(0);
        ExecutionPlan executionPlan = planner.getPlan(ast, pc);

        if (executionPlan == null) {
            return null;
        }
        if (!(executionPlan.getAst().getKind().belongsTo(SqlKind.QUERY) || executionPlan.getAst().getKind()
            .belongsTo(SqlKind.DML))) {
            return null;
        }
        if (!suitableSql(executionPlan.getPlan(), schemaName)) {
            return null;
        }
        return executionPlan;
    }

    /**
     * sharding entry point of unit test
     *
     * @param pc the plan context
     * @param dir the directory of sqls to be analysed
     * @return the result of sharding
     */
    public ShardResultForOutput advise(PlannerContext pc, List<String> dir) {
        List<Map<String, String>> sqls = loadSqls(dir);

        for (Map<String, String> entry : sqls) {
            String testSql = entry.get("sql");
            testSql = testSql.trim();
            int cnt = 1;
            if (entry.containsKey("count")) {
                cnt = Integer.parseInt(replaceBlank(entry.get("count")));
            }
            Planner planner = new RewriterPlanner();
            SqlNodeList astList = new FastsqlParser().parse(testSql, pc.getExecutionContext());
            SqlNode ast = astList.get(0);
            ExecutionPlan executionPlan = planner.getPlan(ast, pc);
            addPlan(executionPlan.getPlan(), cnt);
        }
        return adviseAfterBuild();
    }

    private static class RewriterPlanner extends Planner {
        @Override
        public RelNode optimize(RelNode input, PlannerContext plannerContext) {
            return optimizeBySqlWriter(input, plannerContext);
        }
    }

    public static String replaceBlank(String str) {
        String dest = "";
        if (str != null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }

    private static List<Map<String, String>> loadSqls(List<String> fileNames) {
        List<Map<String, String>> sqls = new ArrayList<>();
        for (String fileName : fileNames) {
            String content = readToString(fileName);
            Yaml yaml = new Yaml();
            Object o = yaml.load(content);
            if (o instanceof Map) {
                if (((Map<?, ?>) o).containsKey("SQL") && ((Map<?, ?>) o).containsKey("DDL")
                    && ((Map<?, ?>) o).containsKey("STATISTICS") && ((Map<?, ?>) o).containsKey("CONFIG")) {
                    o = ((Map<?, ?>) o).get("SQL");
                }
            }
            if (o instanceof List) {
                sqls.addAll((List<Map<String, String>>) o);
            } else {
                sqls.add((Map<String, String>) o);
            }
        }
        return sqls;
    }

    private static String readToString(String fileName) {
        String encoding = "utf-8";
        File file = new File(fileName);
        long fileLength = file.length();
        byte[] fileContent = new byte[(int) fileLength];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(fileContent);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            return new String(fileContent, encoding);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * determine whether the sql can be used to shard
     *
     * @param plan the plan of the sql to be tested
     * @param schema the name of target schema
     * @return true if the sql is suitable
     */
    boolean suitableSql(RelNode plan, String schema) {
        TableScanFinder tsf = new TableScanFinder();
        plan.accept(tsf);
        if (tsf.shouldSkip()) {
            return false;
        }
        List<Pair<String, TableScan>> results = tsf.getResult();
        if (results.size() == 0) {
            return false;
        }
        for (Pair<String, TableScan> result : results) {
            if (!schema.equalsIgnoreCase(result.getKey())) {
                return false;
            }
        }
        return true;
    }

    static public class AdvisorCache {
        long hitCount;
        String sql;
        List<Object> parameters;

        public AdvisorCache(long hitCount, String sql, List<Object> parameters) {
            this.hitCount = hitCount + 1;
            this.sql = sql;
            this.parameters = parameters;
        }

        public long getHitCount() {
            return hitCount;
        }

        public void addHitCount(long hitCount) {
            this.hitCount += hitCount + 1;
        }

        public String getSql() {
            return sql;
        }

        public List<Object> getParameters() {
            return parameters;
        }
    }
}

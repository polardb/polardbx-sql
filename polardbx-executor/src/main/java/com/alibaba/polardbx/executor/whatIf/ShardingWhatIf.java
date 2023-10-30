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

package com.alibaba.polardbx.executor.whatIf;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.sharding.advisor.ShardResultForOutput;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

/**
 * calculate the cost change if the shard key changes
 *
 * @author shengyu
 */
public class ShardingWhatIf {
    private static final Logger logger = LoggerFactory.getLogger(ShardingWhatIf.class);

    List<RelOptCost> oldCosts;
    List<RelOptCost> newCosts;
    List<Pair<SqlParameterized, Integer>> sqls;
    ParamManager paramManager;

    public void whatIf(ShardResultForOutput shardResultForOutput, String schemaName, ParamManager paramManager) {
        this.paramManager = paramManager;

        sqls = shardResultForOutput.getSqls();
        if (sqls.isEmpty()) {
            return;
        }

        if (shardResultForOutput.display().get(schemaName) == null) {
            return;
        }
        // record the shard plan first
        for (Map.Entry<String, StringBuilder> entry : shardResultForOutput.display().entrySet()) {
            logger.info(entry.getKey() + ": " + entry.getValue().toString());
        }

        // what if using the new sharding
        ExecutionContext ec = new ExecutionContext();
        ec.setServerVariables(new HashMap<>());
        ec.setSchemaName(schemaName);

        oldCosts = getCosts(ec, sqls);

        //prepare the whatIf schema
        Map<String, SchemaManager> oldSchemaManagers = ec.getSchemaManagers();
        Map<String, SchemaManager> whatIfSchemaManagers = new TreeMap<>(String::compareToIgnoreCase);
        SchemaManager oldSchemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();

        SchemaManager whatIfSchemaManager =
            new ShardingAdvisorWhatIfSchemaManager(oldSchemaManager,
                shardResultForOutput.display().get(schemaName).toString(),
                DbInfoManager.getInstance().isNewPartitionDb(schemaName),
                ec);
        whatIfSchemaManager.init();
        whatIfSchemaManagers.put(schemaName, whatIfSchemaManager);

        whatIfSchemaManager.getTddlRuleManager().getTddlRule().getVersionedTableNames().clear();
        //calculate the whatIf partition cost
        ec.setSchemaManagers(whatIfSchemaManagers);

        newCosts = getCosts(ec, sqls);

        if (paramManager.getBoolean(ConnectionParams.SHARDING_ADVISOR_RECORD_PLAN)) {
            debugUse(ec, sqls, whatIfSchemaManagers, oldSchemaManagers);
        }
        ec.setSchemaManagers(oldSchemaManagers);
    }

    /**
     * print all plans
     *
     * @param ec ExecutionContext for optimizer
     * @param sqls all sqls to be generated
     * @param whatIfSchemaManagers the whatIf schemas
     * @param oldSchemaManagers current schemas
     */
    private void debugUse(ExecutionContext ec, List<Pair<SqlParameterized, Integer>> sqls,
                          Map<String, SchemaManager> whatIfSchemaManagers,
                          Map<String, SchemaManager> oldSchemaManagers) {
        ec.setCalcitePlanOptimizerTrace(new CalcitePlanOptimizerTrace());
        ec.getCalcitePlanOptimizerTrace().ifPresent(x -> x.setSqlExplainLevel(SqlExplainLevel.ALL_ATTRIBUTES));
        for (Pair<SqlParameterized, Integer> entry : sqls) {
            SqlParameterized sql = entry.getKey();
            // old plan
            Parameters para = new Parameters();
            para.setParams(OptimizerUtils.buildParam(sql.getParameters()));
            ec.setParams(para);
            ec.setSchemaManagers(oldSchemaManagers);
            ExecutionPlan planOld = Planner.getInstance().doBuildPlan(sql, ec);

            RelOptCost costOld = RelMetadataQuery.instance().getCumulativeCost(planOld.getPlan());
            RelDrdsWriter relWriter =
                new RelDrdsWriter(null, SqlExplainLevel.ALL_ATTRIBUTES, false, para.getCurrentParameter(), null, null);
            relWriter.setExecutionContext(ec);
            planOld.getPlan().explainForDisplay(relWriter);
            StringBuilder sb = new StringBuilder("\n");
            sb.append(relWriter.asString()).append("\n");

            //new plan
            para.setParams(OptimizerUtils.buildParam(sql.getParameters()));
            ec.setParams(para);
            ec.setSchemaManagers(whatIfSchemaManagers);
            ExecutionPlan planNew = Planner.getInstance().doBuildPlan(sql, ec);

            RelOptCost costNew = RelMetadataQuery.instance().getCumulativeCost(planNew.getPlan());
            relWriter =
                new RelDrdsWriter(null, SqlExplainLevel.ALL_ATTRIBUTES, false, para.getCurrentParameter(), null, null);
            relWriter.setExecutionContext(ec);
            planNew.getPlan().explainForDisplay(relWriter);
            sb.append(relWriter.asString()).append("\n");
            sb.append(costOld.toString()).append("\n").append(costNew.toString()).append("\n");
            logger.info(sb.toString());
        }
    }

    private List<RelOptCost> getCosts(ExecutionContext ec, List<Pair<SqlParameterized, Integer>> sqls) {
        List<RelOptCost> costs = new ArrayList<>(sqls.size());
        for (Pair<SqlParameterized, Integer> entry : sqls) {
            SqlParameterized sql = entry.getKey();
            // optimize the sql
            Parameters para = new Parameters();
            para.setParams(OptimizerUtils.buildParam(sql.getParameters()));
            ec.setParams(para);
            ExecutionPlan plan = Planner.getInstance().doBuildPlan(sql, ec);

            RelOptCost originalCost = plan.getPlan().getCluster().getMetadataQuery().getCumulativeCost(plan.getPlan());
            costs.add(originalCost);
        }
        return costs;
    }

    /**
     * get the result of whatif
     *
     * @return summary the whatif cost
     */
    public List<String> summarize() {

        if (sqls == null || sqls.isEmpty() || oldCosts == null || newCosts == null) {
            return null;
        }
        int sqlNum = 0;
        Map<CostCmp, Integer> count = new HashMap<>();
        for (CostCmp cmp : CostCmp.values()) {
            count.put(cmp, 0);
        }
        // max heap of cost change(new minus old)
        Queue<Integer> winQueue = new PriorityQueue<>(new SQLComparator(true));

        // min heap of cost change(new minus old)
        Queue<Integer> loseQueue = new PriorityQueue<>(new SQLComparator(false));
        SQLComparator realComparator = new SQLComparator();
        // get top-k cost change
        int topK = 5;
        for (int id = 0; id < oldCosts.size(); id++) {
            RelOptCost oldCost = oldCosts.get(id);
            RelOptCost newCost = newCosts.get(id);
            CostCmp cmp = compareCost(oldCost, newCost);
            int hit = sqls.get(id).getValue();
            sqlNum += hit;
            count.put(cmp, count.get(cmp) + hit);

            if (cmp == CostCmp.WIN) {
                if (winQueue.size() < topK) {
                    winQueue.add(id);
                } else {
                    if (realComparator.compare(winQueue.peek(), id) > 0) {
                        winQueue.poll();
                        winQueue.add(id);
                    }
                }
            }
            if (cmp == CostCmp.LOSE) {
                if (loseQueue.size() < topK) {
                    loseQueue.add(id);
                } else {
                    if (realComparator.compare(loseQueue.peek(), id) < 0) {
                        loseQueue.poll();
                        loseQueue.add(id);
                    }
                }
            }
        }
        List<String> advices = new ArrayList<>();
        String sb = "sqls number: " + sqlNum
            + ", win: " + count.get(CostCmp.WIN)
            + ", tie: " + count.get(CostCmp.TIE)
            + ", lose: " + count.get(CostCmp.LOSE);
        advices.add(sb);
        logger.info(sb);

        //print sql log
        for (int id = 0; id < oldCosts.size(); id++) {
            String sql = getSql(id);
            RelOptCost oldCost = oldCosts.get(id);
            RelOptCost newCost = newCosts.get(id);
            logger.info(sql + "\n" +
                "oldCost: " + oldCost + "\n" +
                "newCost: " + newCost);
        }

        // if the new shard plan is good enough
        if (paramManager.getBoolean(ConnectionParams.SHARDING_ADVISOR_RETURN_ANSWER)
            || (count.get(CostCmp.WIN) >= count.get(CostCmp.LOSE) && count.get(CostCmp.TIE) < (double) sqlNum * 0.8)) {
            advices.add(sqlGen(winQueue));
            advices.add(sqlGen(loseQueue));
            return advices;
        }
        return null;
    }

    /**
     * generate the sql and difference of it's cost
     *
     * @param queue queue of sqls
     * @return string of the result
     */
    private String sqlGen(Queue<Integer> queue) {
        StringBuilder sb = new StringBuilder();
        List<Integer> ids = new ArrayList<>(queue.size());
        while (!queue.isEmpty()) {
            ids.add(queue.poll());
        }

        for (int i = ids.size() - 1; i >= 0; i--) {
            sb.append("\n").append(getSql(ids.get(i))).append("\n");
            sb.append(getCostAdvice(ids.get(i), true)).append("\n");
            logger.info("value: " +
                (((DrdsRelOptCostImpl) newCosts.get(ids.get(i))).getValue() - ((DrdsRelOptCostImpl) oldCosts.get(
                    ids.get(i))).getValue())
                    * sqls.get(ids.get(i)).getValue());
        }
        return sb.toString();
    }

    String getSql(int id) {
        if (id == -1) {
            return "";
        }
        String sql = sqls.get(id).getKey().getSql();
        if (sql != null && sql.length() > 100) {
            sql = sql.substring(0, 100);
        }
        return sql;
    }

    String getCostAdvice(int id, boolean useHit) {
        if (id == -1) {
            return "";
        }
        DrdsRelOptCostImpl afterCost = (DrdsRelOptCostImpl) newCosts.get(id)
            .multiplyBy(useHit ? sqls.get(id).getValue() : 1);
        DrdsRelOptCostImpl beforeCost = (DrdsRelOptCostImpl) oldCosts.get(id)
            .multiplyBy(useHit ? sqls.get(id).getValue() : 1);
        return "VALUE_CHANGE:" +
            (beforeCost.getValue() != 0 ?
                toPercent((afterCost.getValue() - beforeCost.getValue()) / beforeCost.getValue()) : "INF") +
            " CPU_CHANGE:" +
            (afterCost.getCpu() != 0 ?
                toPercent((afterCost.getCpu() - beforeCost.getCpu()) / beforeCost.getCpu()) : "INF") +
            " MEMORY_CHANGE:" +
            (afterCost.getMemory() != 0 ?
                toPercent((afterCost.getMemory() - beforeCost.getMemory()) / beforeCost.getMemory()) : "INF") +
            " IO_CHANGE:" +
            (afterCost.getIo() != 0 ?
                toPercent((afterCost.getIo() - beforeCost.getIo()) / beforeCost.getIo()) : "INF") +
            " NET_CHANGE:" +
            (afterCost.getNet() != 0 ?
                toPercent((afterCost.getNet() - beforeCost.getNet()) / beforeCost.getNet()) : "INF");
    }

    private static String toPercent(double s) {
        DecimalFormat fmt = new DecimalFormat("##0.0%");
        return fmt.format(s);
    }

    /**
     * compare the cost change
     *
     * @param beforeCost cost before change
     * @param afterCost cost after change
     */
    private CostCmp compareCost(RelOptCost beforeCost, RelOptCost afterCost) {
        if (lessThan(afterCost, beforeCost)) {
            return CostCmp.WIN;
        }
        if (lessThan(beforeCost, afterCost)) {
            return CostCmp.LOSE;
        }
        return CostCmp.TIE;
    }

    private boolean lessThan(RelOptCost cost1, RelOptCost cost2) {
        return cost1.isLt(cost2) && (cost1.getIo() < 0.95 * cost2.getIo() || cost1.getNet() < 0.95 * cost2.getNet());
    }

    private enum CostCmp {
        LOSE, TIE, WIN
    }

    class SQLComparator implements Comparator<Integer> {

        boolean reverse;

        SQLComparator() {
            this.reverse = false;
        }

        SQLComparator(boolean reverse) {
            this.reverse = reverse;
        }

        @Override
        public int compare(Integer a, Integer b) {
            return reverse ? -compareReal(a, b) : compareReal(a, b);
        }

        public int compareReal(Integer a, Integer b) {
            // '(new[a]-old[a])*hit[a] > (new[b]-old[b])*hit[b]' is transformed to
            // 'new[b]*hit[b] + old[a]*hit[a] < new[a]*hit[a] + old[b]*hit[b]'
            RelOptCost rightCost = newCosts.get(a).multiplyBy(sqls.get(a).getValue())
                .plus(oldCosts.get(b).multiplyBy(sqls.get(b).getValue()));

            RelOptCost leftCost = newCosts.get(b).multiplyBy(sqls.get(b).getValue())
                .plus(oldCosts.get(a).multiplyBy(sqls.get(a).getValue()));
            if (leftCost.isLt(rightCost)) {
                return 1;
            }

            if (leftCost.equals(rightCost)) {
                return 0;
            }
            return -1;
        }
    }
}

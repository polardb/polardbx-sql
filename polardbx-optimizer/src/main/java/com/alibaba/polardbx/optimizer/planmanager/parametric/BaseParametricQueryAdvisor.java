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

package com.alibaba.polardbx.optimizer.planmanager.parametric;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MINOR_TOLERANCE_VALUE;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.isTolerated;
import static com.alibaba.polardbx.optimizer.planmanager.parametric.SimilarityAlgo.EUCLIDEAN;

/**
 * @author fangwu
 */
public class BaseParametricQueryAdvisor implements ParametricQueryAdvisor {

    public static final double MAX_POINTS_NUM_EACH_BASELINE = 3;

    public static final ParametricQueryAdvisor pqa = new BaseParametricQueryAdvisor();

    private BaseParametricQueryAdvisor() {
    }

    public static ParametricQueryAdvisor getInstance() {
        return pqa;
    }

    @Override
    public void feedBack(PlannerContext plannerContext, BaselineInfo baselineInfo, PlanInfo planInfo,
                         ExecutionContext executionContext,
                         Map<RelNode, RuntimeStatisticsSketch> runtimeStatistic) {
        /**
         * parameter -> cardinality
         */
        Point p = executionContext.getPoint();
        if (p == null) {
            return;
        }
        final double runtimeRowCount =
            runtimeStatistic.entrySet().stream().mapToDouble(r -> r.getValue().getRowCount()).sum();

        /**
         * check the need of adding candidate point
         */
        if (baselineInfo != null &&
            baselineInfo.getPointSet() != null &&
            baselineInfo.getPointSet().size() <= MAX_POINTS_NUM_EACH_BASELINE &&
            plannerContext.getExprMap() != null &&
            !isTolerated(p.getRowcountExpected(),
                runtimeRowCount,
                executionContext.getParamManager().getInt(MINOR_TOLERANCE_VALUE))) {
            List<Object> params = Lists.newArrayList();
            for (int i = 1; i <= executionContext.getParams().getCurrentParameter().size(); i++) {
                params.add(executionContext.getParams().getCurrentParameter().get(i).getValue());
            }
            if (params.size() == 0) {
                // should not happen
                return;
            }
            Point candidate =
                new Point(p.getParameterSql(), getFullSelectivity(plannerContext.getExprMap(), params), params);
            candidate.feedBackRowcount(runtimeRowCount);
            return;
        }

        // record rowcount when first executing
        p.feedBackRowcount(runtimeRowCount);
    }

    @Override
    public Pair<Point, PlanInfo> advise(String parameterSql, Collection<PlanInfo> planInfos,
                                        List<Object> params,
                                        Collection<Point> points,
                                        PlannerContext plannerContext,
                                        int currHashCode) {
        assert planInfos != null && planInfos.size() > 0;
        Map<String, Double> fullSelectivity = getFullSelectivity(plannerContext.getExprMap(), params);

        Point p = mostLikelyPoint(points, parameterSql, params, fullSelectivity);

        p.increaseChooseTime(1);
        if (p.getPlanId() == -1L) {
            return Pair.of(p, null);
        }

        for (PlanInfo planInfo : planInfos) {
            if (planInfo.getTablesHashCode() == currHashCode && planInfo.getId() == p.getPlanId()) {
                return Pair.of(p, planInfo);
            }
        }

        // plan is not exists
        p.setPlanId(-1);
        return Pair.of(p, null);
    }

    private Map<String, Double> getFullSelectivity(Map<LogicalTableScan, RexNode> exprMap,
                                                   List<Object> params) {
        Map<String, Double> m = Maps.newTreeMap();
        for (Map.Entry<LogicalTableScan, RexNode> r : exprMap.entrySet()) {
            double selectivity = RelMetadataQuery.instance().getSelectivity(r.getKey(), r.getValue());
            String tableName = r.getKey().getTable().getQualifiedName().toString();
            if (m.containsKey(tableName)) {
                m.put(tableName, selectivity + m.get(tableName));
            } else {
                m.put(tableName, selectivity);
            }
        }
        return m;
    }

    private Point mostLikelyPoint(Collection<Point> pointSet, String parameterSql, List<Object> params,
                                  Map<String, Double> fullSelectivity) {
        if (pointSet == null || pointSet.size() == 0) {
            Point p = new Point(parameterSql, fullSelectivity, params);
            pointSet = Sets.newConcurrentHashSet();
            pointSet.add(p);
            return p;
        } else if (pointSet.size() == 1) {
            return pointSet.iterator().next();
        } else {
            return nearestPoint(pointSet, fullSelectivity);
        }
    }

    private Point nearestPoint(Collection<Point> pointSet, Map<String, Double> fullSelectivity) {
        double lowestDistance = Double.MAX_VALUE;
        Point nearestPoint = pointSet.iterator().next();
        int count = 0;
        double[] selectivityArray = new double[fullSelectivity.size()];
        for (double selectivity : fullSelectivity.values()) {
            selectivityArray[count++] = selectivity;
        }
        for (Point p : pointSet) {
            double distance = -1;
            if (fullSelectivity.size() >= 2) {
                distance = EUCLIDEAN.distance(p.getSelectivityArray(), selectivityArray);
            } else if (fullSelectivity.size() == 1) {
                distance = Math.abs(p.getSelectivityArray()[0] - selectivityArray[0]);
            }

            distance = p.getInflationNarrow() * distance;
            if (lowestDistance > distance) {
                lowestDistance = distance;
                nearestPoint = p;
            }
        }
        return nearestPoint;
    }

}

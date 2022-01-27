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

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.PredicateNode;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MINOR_TOLERANCE_VALUE;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.isTolerated;

/**
 * @author fangwu
 */
public class MyParametricQueryAdvisor implements ParametricQueryAdvisor {

    public static final double INFLATION_FACTOR = 1.3;
    public static final double INFLATION_NARROW_MAX = 10;
    public static final double STEADY_CHOOSE_TIME = 100;
    public static final double UNUSE_ELIMITE_TIME = 10;
    public static final double UNUSE_ELIMITE_SECONDS = 60 * 60 * 24 * 7;
    public static final double RECENTLY_CHOOSE_RATE_SCOPE = 100;
    private Map<String, Set<Point>> parameterSqlMap = Maps.newConcurrentMap();
    private SimilarityAlgo similarityAlgo;
    private Map<Point, Long> pointUnuseTime = Maps.newHashMap();

    public MyParametricQueryAdvisor(SimilarityAlgo similarityAlgo) {
        this.similarityAlgo = similarityAlgo;
    }

    @Override
    public void feedBack(PlannerContext plannerContext, PlanInfo planInfo, ExecutionContext executionContext,
                         Map<RelNode, RuntimeStatisticsSketch> runtimeStatistic) {
        /**
         * parameter -> cardinality
         */
        Point p = executionContext.getPoint();
        if (p == null) {
            return;
        }
        final double[] runtimeRowCount = new double[1];
        runtimeStatistic.entrySet().forEach(relNodeRuntimeStatisticsSketchEntry -> runtimeRowCount[0] +=
            relNodeRuntimeStatisticsSketchEntry.getValue().getRowCount());

        /**
         * check the need of adding candidate point
         */
        if (plannerContext.getExprMap() != null &&
            !PlanManagerUtil.isSteady(p) &&
            !isTolerated(p.getRowcountExpected(),
                runtimeRowCount[0],
                executionContext.getParamManager().getInt(MINOR_TOLERANCE_VALUE)) &&
            parameterSqlMap.get(p.getParameterSql()).size() < 5) {
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
            parameterSqlMap.get(p.getParameterSql()).add(candidate);
            candidate.feedBackRowcount(runtimeRowCount[0]);
            return;
        }

        // record rowcount when first executing
        p.feedBackRowcount(runtimeRowCount[0]);
//        if (!isTolerated(p.getPhyFeedBack().getExaminedRowCount(),
//            executionContext.getXFeedBack().getExaminedRowCount(),
//            executionContext.getParamManager().getInt(MINOR_TOLERANCE_VALUE))) {
//            // TODO 发起统计信息收集
//        }
    }

    @Override
    public Pair<Point, PlanInfo> advise(String parameterSql, Collection<PlanInfo> planInfos,
                                        List<Object> params,
                                        PlannerContext plannerContext,
                                        ExecutionContext executionContext, boolean isExplain) {
        assert planInfos != null && planInfos.size() > 0;
        Map<String, Double> fullSelectivity = getFullSelectivity(plannerContext.getExprMap(), params);

        Point p = mostLikelyPoint(parameterSql, params, fullSelectivity, parameterSqlMap, isExplain);

        p.increaseChooseTime(1);
        if (p.getPlanId() == -1L) {
            return Pair.of(p, null);
        }

        for (PlanInfo planInfo : planInfos) {
            if (planInfo.getId() == p.getPlanId()) {
                return Pair.of(p, planInfo);
            }
        }

        // plan is not exists
        p.setPlanId(-1);
        return Pair.of(p, null);
    }

    @Override
    public void load(Map<String, Set<Point>> points) {
        parameterSqlMap = points;
    }

    @Override
    public void checkPlanRedundant(int minorTolerance) {
        for (String sql : parameterSqlMap.keySet()) {
            mergePoint(parameterSqlMap.get(sql));
            List<Point> toRemove = Lists.newArrayList();
            for (Point p : parameterSqlMap.get(sql)) {
                if (p.getUnuseTermTime() >= UNUSE_ELIMITE_TIME) {
                    toRemove.add(p);
                }
                if (!(isTolerated(p.getRowcountExpected(), p.getMaxRowcountExpected(), minorTolerance) && isTolerated(
                    p.getRowcountExpected(), p.getMinRowcountExpected(), minorTolerance))) {
                    if (!PlanManagerUtil.isSteady(p)) {
                        p.setInflationNarrow(p.getInflationNarrow() * INFLATION_FACTOR);
                        p.setMaxRowcountExpected(p.getRowcountExpected());
                        p.setMinRowcountExpected(p.getRowcountExpected());
                    }
                }
            }

            /**
             * cal choose rate
             */
            int recentlyChooseSum =
                parameterSqlMap.get(sql).stream().mapToInt(point -> point.getRecentlyChooseTime()).sum();
            if (recentlyChooseSum > RECENTLY_CHOOSE_RATE_SCOPE) {
                parameterSqlMap.get(sql).stream()
                    .forEach(point -> point.updateLastRecentlyChooseTime(recentlyChooseSum));
            }

            /**
             * handle unuse point
             */
            for (Point p : parameterSqlMap.get(sql)) {
                if (p.getLastRecentlyChooseRate() < 0.01) {
                    if (pointUnuseTime.get(p) == null) {
                        pointUnuseTime.put(p, System.currentTimeMillis());
                    } else if (System.currentTimeMillis() - pointUnuseTime.get(p) > UNUSE_ELIMITE_SECONDS * 1000) {
                        toRemove.add(p);
                    }
                } else {
                    pointUnuseTime.remove(p);
                }
            }

            parameterSqlMap.get(sql).removeAll(toRemove);
        }

    }

    @Override
    public Map<String, Set<Point>> dump() {
        return parameterSqlMap;
    }

    @Override
    public void remove(String parameterSql) {
        parameterSqlMap.remove(parameterSql);
    }

    @Override
    public void clear() {
        parameterSqlMap.clear();
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

    private Point mostLikelyPoint(String parameterSql, List<Object> params,
                                  Map<String, Double> fullSelectivity,
                                  Map<String, Set<Point>> parameterSqlMap, boolean isExplain) {
        Set<Point> pointSet = parameterSqlMap.get(parameterSql);
        if (pointSet == null || pointSet.size() == 0) {
            Point p = new Point(parameterSql, fullSelectivity, params);
            pointSet = Sets.newConcurrentHashSet();
            pointSet.add(p);
            if (!isExplain) {
                parameterSqlMap.put(parameterSql, pointSet);
            }
            return p;
        } else if (pointSet.size() == 1) {
            return pointSet.iterator().next();
        } else {
            return mostLikeLyPoint(pointSet, fullSelectivity);
        }
    }

    private void mergePoint(Set<Point> pointSetTmp) {
        Set<Point> pointSet = Sets.newHashSet(pointSetTmp);
        Set<Point> toRemove = Sets.newHashSet();
        List<Long> existsPlanId = Lists.newArrayList();
        Point[] points = pointSet.toArray(new Point[0]);
        Arrays.sort(points, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                return o1.getChooseTime() > o2.getChooseTime() ? -1 : 1;
            }
        });

        List<double[]> existsSelectivity = Lists.newArrayList();
        for (Point p : pointSet) {
            for (double[] dArrays : existsSelectivity) {
                if (Arrays.equals(dArrays, p.getSelectivityArray())) {
                    toRemove.add(p);
                    continue;
                }
            }
            existsSelectivity.add(p.getSelectivityArray());

            if (existsPlanId.contains(p.getPlanId())) {
                toRemove.add(p);
                continue;
            }
            if (p.getPlanId() != -1) {
                existsPlanId.add(p.getPlanId());
            }
        }
        pointSetTmp.removeAll(toRemove);
    }

    private Point mostLikeLyPoint(Set<Point> pointSet, Map<String, Double> fullSelectivity) {
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
                distance = similarityAlgo.distance(p.getSelectivityArray(), selectivityArray);
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

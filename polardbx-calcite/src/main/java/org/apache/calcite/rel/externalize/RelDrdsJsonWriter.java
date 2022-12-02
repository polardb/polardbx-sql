/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.externalize;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.ParameterContext;

public class RelDrdsJsonWriter implements RelWriter {

    public final static String                   REL_NAME      = RelDrdsWriter.REL_NAME;
    public final static String                   LV_INPUTS     = RelDrdsWriter.LV_INPUTS;

    private final JsonBuilder                    jsonBuilder   = new JsonBuilder();
    private final SqlExplainLevel                detailLevel;
    private final List<Pair<String, Object>>     values        = new ArrayList<>();
    private final Map<Integer, ParameterContext> params;
    private boolean                              isFull        = false;
    private Function<RexNode, Object>            funcEvaluator;
    private Map<String, Object> currentNode;
    private Object                            executionContext = null;

    private CalcitePlanOptimizerTrace calcitePlanOptimizerTrace = null;
    public RelDrdsJsonWriter(SqlExplainLevel detailLevel,
                             Map<Integer, ParameterContext> params,
                             Function<RexNode, Object> funcEvaluator,
                             Object executionContext,
                             CalcitePlanOptimizerTrace calcitePlanOptimizerTrace){
        this.detailLevel = detailLevel;
        this.params = params;
        this.funcEvaluator = funcEvaluator;
        this.executionContext = executionContext;
        this.calcitePlanOptimizerTrace = calcitePlanOptimizerTrace;
    }


    // ~ Methods
    // ----------------------------------------------------------------
    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        List<RelNode> inputs = rel.getInputs();

        if (inputs.isEmpty()) {
            for (Pair<String, Object> val : values) {
                String inputsOfLogicalView = val.getKey();
                if (inputsOfLogicalView.equals(LV_INPUTS)) {
                    inputs = (List<RelNode>) val.getValue();
                }
            }
        }

        Map<String, Object> node = jsonBuilder.map();
        node.put("_type", null);
        Map<String, Object> props = jsonBuilder.map();
        node.put("props", props);

        for (Pair<String, Object> value : values) {
            if (value.right instanceof RelNode) {
                continue;
            }
            if (REL_NAME.equals(value.left)) {
                node.put("_type", value.right); // override the 'null'
                continue;
            }
            if (LV_INPUTS.equals(value.left)) {
                continue;
            }
            if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
                props.put(value.left, String.valueOf(value.right));
            }
        }

        if (detailLevel == SqlExplainLevel.ALL_ATTRIBUTES) {
            RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
            node.put("rowcount", mq.getRowCount(rel));
            node.put("cumulative_cost", mq.getCumulativeCost(rel));
            node.put("noncumulative_cost", mq.getNonCumulativeCost(rel));
        }

        // Runtime statistics displayed in EXPLAIN ANALYZE
        Map<RelNode, RuntimeStatisticsSketch> statistics = calcitePlanOptimizerTrace == null ? null :
            calcitePlanOptimizerTrace.getOptimizerTracer().getRuntimeStatistics();
        if (statistics != null) {
            RuntimeStatisticsSketch sketch = statistics.get(rel);
            if (sketch != null) {
                node.put("actual_open_time", sketch.getStartupDuration());
                node.put("actual_next_time", sketch.getDuration());
                node.put("actual_worker_time", sketch.getWorkerDuration());
                node.put("actual_rowcount", sketch.getRowCount());
                node.put("actual_memory", sketch.getMemory());

                if (sketch.getSpillCnt() > 0) {
                    node.put("spill count = ", sketch.getSpillCnt());
                }

                node.put("instances", sketch.getInstances());
            }
        }

        switch (detailLevel) {
            case NON_COST_ATTRIBUTES:
            case ALL_ATTRIBUTES:
                node.put("id", rel.getId());
                break;
        }

        if (!inputs.isEmpty()) {
            List<Object> inputNodes = jsonBuilder.list();
            for (RelNode input : inputs) {
                input.explainForDisplay(this);
                inputNodes.add(currentNode);
            }
            node.put("inputs", inputNodes);
        }

        currentNode = node;
    }

    public final void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        explain_(rel, valueList);
    }

    public SqlExplainLevel getDetailLevel() {
        return detailLevel;
    }

    public RelWriter input(String term, RelNode input) {
        values.add(Pair.of(term, (Object) input));
        return this;
    }

    public RelWriter item(String term, Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    public RelWriter itemIf(String term, Object value, boolean condition) {
        if (condition) {
            item(term, value);
        }
        return this;
    }

    public RelWriter done(RelNode node) {
        // 实际实现上，并没有保证 EXPLAIN 结果中一定包含所有 INPUT，在实现修改好之前，先注释掉
        // assert checkInputsPresentInExplain(node);
        final List<Pair<String, Object>> valuesCopy = ImmutableList.copyOf(values);
        values.clear();
        explain_(node, valuesCopy);
        return this;
    }

    public boolean nest() {
        return false;
    }

    public String asString() {
        return jsonBuilder.toJsonString(currentNode);
    }

    public Map<Integer, ParameterContext> getParams() {
        return params;
    }

    public boolean isFull() {
        return isFull;
    }

    public void setFull(boolean isFull) {
        this.isFull = isFull;
    }

    public Function<RexNode, Object> getFuncEvaluator() {
        return funcEvaluator;
    }

    public Object getExecutionContext() {
        return executionContext;
    }

    public void setExecutionContext(Object executionContext) {
        this.executionContext = executionContext;
    }
}

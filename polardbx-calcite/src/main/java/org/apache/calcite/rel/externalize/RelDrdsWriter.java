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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import org.apache.calcite.avatica.util.Spacer;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author 梦实 2017年10月24日 下午3:16:21
 * @since 5.0.0
 */
public class RelDrdsWriter implements RelWriter {

    public final static String                   REL_NAME      = "REL_NAME";
    public final static String                   LV_INPUTS     = "LV_INPUTS";
    // ~ Instance fields
    // --------------------------------------------------------
    private StringWriter                         sw            = new StringWriter();
    protected PrintWriter                        pw            = new PrintWriter(sw);
    private final SqlExplainLevel                detailLevel;
    private final boolean                        withIdPrefix;
    protected final Spacer                       spacer        = new Spacer();
    private final List<Pair<String, Object>>     values        = new ArrayList<>();
    private final Map<Integer, ParameterContext> params;
    private boolean                              isFull        = false;
    private Function<RexNode, Object>            funcEvaluator = null;
    private Function<RelNode, String>         extraInfoBuilder = null;
    private List<Object>                         extraInfos    = null;
    private Object                            executionContext = null;

    private CalcitePlanOptimizerTrace calcitePlanOptimizerTrace = null;
    // ~ Constructors
    // -----------------------------------------------------------

    public RelDrdsWriter(PrintWriter pw, SqlExplainLevel detailLevel, Map<Integer, ParameterContext> params, Function<RexNode, Object> funcEvaluator, Object executionContext){
        this(pw, detailLevel, false, params, funcEvaluator, executionContext);
    }

    public RelDrdsWriter(PrintWriter pw, SqlExplainLevel detailLevel, boolean withIdPrefix,
                         Map<Integer, ParameterContext> params, Function<RexNode, Object> funcEvaluator, Object executionContext){
        this(pw, detailLevel, withIdPrefix, params, funcEvaluator, null, executionContext);
    }

    public RelDrdsWriter(PrintWriter pw, SqlExplainLevel detailLevel, boolean withIdPrefix,
                         Map<Integer, ParameterContext> params, Function<RexNode, Object> funcEvaluator, Function<RelNode, String> extraInfoBuilder,
                         Object executionContext){
        this(pw, detailLevel, withIdPrefix, params, funcEvaluator, extraInfoBuilder, executionContext, null);
    }
    public RelDrdsWriter(PrintWriter pw, SqlExplainLevel detailLevel, boolean withIdPrefix,
                         Map<Integer, ParameterContext> params, Function<RexNode, Object> funcEvaluator, Function<RelNode, String> extraInfoBuilder,
                         Object executionContext, CalcitePlanOptimizerTrace calcitePlanOptimizerTrace){
        this.pw = pw == null ? this.pw : pw;
        this.detailLevel = detailLevel;
        this.withIdPrefix = withIdPrefix;
        this.params = params;
        this.funcEvaluator = funcEvaluator;
        this.extraInfoBuilder = extraInfoBuilder;
        if (this.extraInfoBuilder != null) {
            extraInfos = new ArrayList<>();
        }
        this.executionContext = executionContext;
        this.calcitePlanOptimizerTrace = calcitePlanOptimizerTrace;
    }

    public RelDrdsWriter(SqlExplainLevel detailLevel){
        this(null, detailLevel, false, null, null, null);
    }

    public RelDrdsWriter(SqlExplainLevel detailLevel, Map<Integer, ParameterContext> params){
        this(null, detailLevel, false, params, null, null);
    }
    public RelDrdsWriter(){
        this(null, SqlExplainLevel.EXPPLAN_ATTRIBUTES, false, null, null, null);
    }

    public void setCalcitePlanOptimizerTrace(CalcitePlanOptimizerTrace calcitePlanOptimizerTrace) {
        this.calcitePlanOptimizerTrace = calcitePlanOptimizerTrace;
    }

    // ~ Methods
    // ----------------------------------------------------------------

    protected void explain_(RelNode rel, List<Pair<String, Object>> values) {
        RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        List<RelNode> inputs = rel.getInputs();

        if (inputs.isEmpty()) {
            for (Pair<String, Object> val : values) {
                String inputsOfLogicalView = val.getKey();
                if (inputsOfLogicalView.equals(LV_INPUTS)) {
                    inputs = (List<RelNode>) val.getValue();
                }
            }
        }

        if (!mq.isVisibleInExplain(rel, detailLevel)) {
            // render children in place of this, at same level
            explainInputs(inputs);
            return;
        }

        StringBuilder s = new StringBuilder();
        spacer.spaces(s);
        if (withIdPrefix) {
            s.append(rel.getId()).append(":");
        }

        // s.append(rel.getRelTypeName()).append(":");
        if (detailLevel != SqlExplainLevel.NO_ATTRIBUTES) {
            int j = 0;
            for (Pair<String, Object> value : values) {
                if (value.right instanceof RelNode) {
                    continue;
                }

                if (REL_NAME == value.left) {
                    s.append(value.right);
                    continue;
                }

                if (LV_INPUTS == value.left) {
                    continue;
                }

                if (j++ == 0) {
                    s.append("(");
                } else {
                    s.append(", ");
                }

                s.append(value.left).append("=");

                if (value.right instanceof String) {
                    s.append("\"").append(value.right).append("\"");
                } else {
                    s.append(value.right);
                }
            }
            if (j > 0) {
                s.append(")");
            }
        }
        if (detailLevel == SqlExplainLevel.ALL_ATTRIBUTES) {
            s.append(": rowcount = ")
                    .append(Math.ceil(mq.getRowCount(rel)))
                    .append(", cumulative cost = ")
                    .append(mq.getCumulativeCost(rel));
        }

        // Runtime statistics displayed in EXPLAIN ANALYZE
        Map<RelNode, RuntimeStatisticsSketch> statistics = calcitePlanOptimizerTrace == null ? null :
            calcitePlanOptimizerTrace.getOptimizerTracer().getRuntimeStatistics();
        if (statistics != null) {
            RuntimeStatisticsSketch sketch = statistics.get(rel);
            if (sketch != null) {
                s.append(", actual time = ").append(String.format("%.3f", sketch.getStartupDuration()))
                        .append(" + ").append(String.format("%.3f", sketch.getDuration()));
                s.append(", actual rowcount = ").append(sketch.getRowCount());
                s.append(", actual memory = ").append(sketch.getMemory());
                if (sketch.getSpillCnt() > 0) {
                    s.append(", spill count = ").append(sketch.getSpillCnt());
                }

                s.append(", instances = ").append(sketch.getInstances());
            }
        }

//        switch (detailLevel) {
//            case NON_COST_ATTRIBUTES:
//            case ALL_ATTRIBUTES:
//                if (!withIdPrefix) {
//                    // If we didn't print the rel id at the start of the line,
//                    // print
//                    // it at the end.
//                    s.append(", id = ").append(rel.getId());
//                }
//                break;
//        }
        pw.println(s);
        spacer.add(2);

        // LogicalInsert have an input of LogicalValue. So stupid and just hide it
        if (!(rel instanceof TableModify && inputs.size() == 1 && (inputs.get(0) instanceof LogicalValues ||
            inputs.get(0) instanceof DynamicValues))) {
            explainInputs(inputs);
        }

        spacer.subtract(2);
    }

    private void explainInputs(List<RelNode> inputs) {
        for (RelNode input : inputs) {
            input.explainForDisplay(this);
        }
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

        // handle extra info
        if (extraInfoBuilder != null && extraInfos != null) {
            Object extraInfo = extraInfoBuilder.apply(node);
            extraInfos.add(extraInfo);
        }

        pw.flush();
        return this;
    }

    private boolean checkInputsPresentInExplain(RelNode node) {
        int i = 0;
        if (values.size() > 0 && values.get(0).left.equals("subset")) {
            ++i;
        }
        for (RelNode input : node.getInputs()) {
            assert values.get(i).right == input;
            ++i;
        }
        return true;
    }

    public boolean nest() {
        return false;
    }

    /**
     * Converts the collected terms and values to a string. Does not write to
     * the parent writer.
     */
    public String simple() {
        final StringBuilder buf = new StringBuilder("(");
        for (Ord<Pair<String, Object>> ord : Ord.zip(values)) {
            if (ord.i > 0) {
                buf.append(", ");
            }
            buf.append(ord.e.left).append("=[").append(ord.e.right).append("]");
        }
        buf.append(")");
        return buf.toString();
    }

    public String asString() {
        if (pw == null) return null;
        String retString = sw.toString();
        return retString;
    }

    public List<Object> getExtraInfos() {
        return this.extraInfos;
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

// End RelWriterImpl.java

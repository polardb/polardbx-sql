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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlAggFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class PushDownAggCtx {

    private int curAvgCount;
    private int currAggCallPos;
    private boolean needNewAgg;

    private LogicalView tableScan;
    private LogicalAggregate aggregate;
    private List<Integer> avgIndex;
    private List<Integer> currArgList;
    private List<AggregateCall> aggCalls;
    private TreeMap<Integer, AggregateCall> newAggCalls;
    private TreeMap<Integer, AggregateCall> pushedAggCalls;
    private TreeMap<Integer, AggregateCall> pushedToPartitionAggCalls;

    public PushDownAggCtx(LogicalView tableScan, LogicalAggregate aggregate) {
        this.tableScan = tableScan;
        this.aggregate = aggregate;
        this.needNewAgg = false;
        this.curAvgCount = 0;
        this.avgIndex = new ArrayList<>();

        this.newAggCalls = new TreeMap<>();
        this.pushedAggCalls = new TreeMap<>();
        this.pushedToPartitionAggCalls = new TreeMap<>();
        this.aggCalls = this.aggregate.getAggCallList();

        this.currArgList = new ArrayList<>();
    }

    public RexBuilder getRexBuilder() {
        return tableScan.getCluster().getRexBuilder();
    }

    public int getCurAvgCount() {
        return this.curAvgCount;
    }

    public int getAggCallCount() {
        return aggCalls.size();
    }

    public boolean getNeedNewAgg() {
        return this.needNewAgg;
    }

    public int getGroupSetLen() {
        return aggregate.getGroupSet().asList().size();
    }

    public List<Integer> getAvgIndex() {
        return this.avgIndex;
    }

    public int getCurrAggCallPos() {
        return this.currAggCallPos;
    }

    public LogicalView getTableScan() {
        return this.tableScan;
    }

    public LogicalAggregate getAggregate() {
        return this.aggregate;
    }

    public List<AggregateCall> getAggCalls() {
        return this.aggCalls;
    }

    public void setNeedNewAgg(boolean needNewAgg) {
        this.needNewAgg = needNewAgg;
    }

    public TreeMap<Integer, AggregateCall> getNewAggCalls() {
        return this.newAggCalls;
    }

    public TreeMap<Integer, AggregateCall> getPushedAggCalls() {
        return this.pushedAggCalls;
    }

    public TreeMap<Integer, AggregateCall> getPushedToPartitionAggCalls() {
        return this.pushedToPartitionAggCalls;
    }

    public AggregateCall getOneAggCall(int i) {
        this.currAggCallPos = i;
        return this.aggCalls.get(i);
    }

    public List<Integer> getCurrArgList() {
        return this.currArgList;
    }

    public RelOptCluster getCluster() {
        return tableScan.getCluster();
    }

    public void addPushedAggCalls(int index, AggregateCall aggCall) {
        pushedAggCalls.put(index, aggCall);
    }

    public void addNewAggCalls(int index, AggregateCall aggCall) {
        newAggCalls.put(index, aggCall);
    }

    public void addPushedToPartitionAggCalls(int index, AggregateCall aggCall) {
        pushedToPartitionAggCalls.put(index, aggCall);
    }

    public void addAvgIndex(int index, int element) {
        avgIndex.add(index, element);
    }

    public AggregateCall getOneDistinctAggCall() {
        for (int i = 0; i < this.aggCalls.size(); ++i) {
            if (true == this.aggCalls.get(i).isDistinct()) {
                this.currAggCallPos = i;
                this.currArgList = this.aggCalls.get(i).getArgList();
                return this.aggCalls.get(i);
            }
        }
        return null;
    }

    public void curAvgCountPlusOne() {
        this.curAvgCount += 1;
    }

    public AggregateCall create(SqlAggFunction func) {
        AggregateCall currAgg = aggCalls.get(this.currAggCallPos);
        return AggregateCall.create(func,
            currAgg.isDistinct(),
            ImmutableList.of(getGroupSetLen() + currAggCallPos),
            currAgg.filterArg,
            currAgg.getType(),
            currAgg.getName());
    }

    public AggregateCall create(SqlAggFunction func, boolean isDistinct) {
        AggregateCall currAgg = aggCalls.get(this.currAggCallPos);
        return AggregateCall.create(func,
            isDistinct,
            ImmutableList.of(getGroupSetLen() + currAggCallPos),
            currAgg.filterArg,
            currAgg.getType(),
            currAgg.getName());
    }

    public AggregateCall create(SqlAggFunction func, List<Integer> argList) {
        AggregateCall currAgg = aggCalls.get(this.currAggCallPos);
        return AggregateCall.create(func,
            currAgg.isDistinct(),
            argList,
            currAgg.filterArg,
            currAgg.getType(),
            currAgg.getName());
    }

    public AggregateCall create(SqlAggFunction func, List<Integer> argList, String name) {
        AggregateCall currAgg = aggCalls.get(this.currAggCallPos);
        return AggregateCall.create(func,
            currAgg.isDistinct(),
            argList,
            currAgg.filterArg,
            currAgg.getType(),
            name);
    }

    public AggregateCall create(SqlAggFunction func, List<Integer> argList, RelDataType relDataType, String name) {
        AggregateCall currAgg = aggCalls.get(this.currAggCallPos);
        return AggregateCall.create(func,
            currAgg.isDistinct(),
            argList,
            currAgg.filterArg,
            relDataType,
            name);
    }
}

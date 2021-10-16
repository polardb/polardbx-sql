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

package com.alibaba.polardbx.optimizer.sharding;

import com.alibaba.polardbx.optimizer.sharding.label.AggregateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.CorrelateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.JoinLabel;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.ProjectLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.sharding.label.TableScanLabel;
import com.alibaba.polardbx.optimizer.sharding.label.UnionLabel;
import com.alibaba.polardbx.optimizer.sharding.label.ValuesLabel;
import org.apache.calcite.linq4j.Ord;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class LabelShuttleImpl implements LabelShuttle {

    protected final Deque<Label> stack = new ArrayDeque<>();

    /**
     * Visits a particular child of a parent.
     */
    protected Label visitChild(Label parent, int i, Label child) {
        stack.push(parent);
        try {
            Label child2 = child.accept(this);
            if (child2 != child) {
                final List<Label> newInputs = new ArrayList<>(parent.getInputs());
                newInputs.set(i, child2);
                return parent.copy(newInputs);
            }
            return parent;
        } finally {
            stack.pop();
        }
    }

    protected Label visitChildren(Label label) {
        for (Ord<Label> input : Ord.zip(label.getInputs())) {
            label = visitChild(label, input.i, input.e);
        }
        return label;
    }

    protected Label parent() {
        return stack.peek();
    }

    @Override
    public Label visit(TableScanLabel tableScanLabel) {
        return visitChildren(tableScanLabel);
    }

    @Override
    public Label visit(ProjectLabel projectLabel) {
        return visitChildren(projectLabel);
    }

    @Override
    public Label visit(JoinLabel joinLabel) {
        return visitChildren(joinLabel);
    }

    @Override
    public Label visit(AggregateLabel aggregateLabel) {
        return visitChildren(aggregateLabel);
    }

    @Override
    public Label visit(UnionLabel unionLabel) {
        return visitChildren(unionLabel);
    }

    @Override
    public Label visit(ValuesLabel valuesLabel) {
        return visitChildren(valuesLabel);
    }

    @Override
    public Label visit(SubqueryLabel subqueryLabel) {
        return visitChildren(subqueryLabel);
    }

    @Override
    public Label visit(CorrelateLabel correlate) {
        return visitChildren(correlate);
    }

    @Override
    public Label visit(SubqueryWrapperLabel subqueryWrapperLabel) {
        return visitChildren(subqueryWrapperLabel);
    }

    @Override
    public Label visit(Label other) {
        return visitChildren(other);
    }
}

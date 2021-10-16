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

package com.alibaba.polardbx.optimizer.sharding.label;

import java.util.List;

import javax.annotation.Nonnull;

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mapping;

import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;

/**
 * Label for the project which is not a permutation
 *
 * @author chenmo.cm
 * @date 2019-08-13 13:04
 */
public class ProjectLabel extends SnapshotLabel {

    protected ProjectLabel(Project rel, Label input, ExtractorContext context){
        super(LabelType.PROJECT, rel, input);

        // immediately apply current project to snapshot label
        project(rel, context);
    }

    protected ProjectLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                           Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                           PredicateNode pushdown, PredicateNode[] columnConditionMap,
                           List<PredicateNode> predicates){
        super(type,
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates);
    }

    public static ProjectLabel create(@Nonnull Project project, Label input, ExtractorContext context) {
        return new ProjectLabel(project, input, context);
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new ProjectLabel(getType(),
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates);
    }

    @Override
    public Label accept(LabelShuttle shuttle) {
        return shuttle.visit(this);
    }
}

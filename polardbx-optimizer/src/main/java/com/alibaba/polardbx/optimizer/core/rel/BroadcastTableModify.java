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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author minggong 2018-4-24 11:22
 * <p>
 * UPDATE / DELETE broadcast table.
 */
public class BroadcastTableModify extends AbstractRelNode {
    private DirectTableOperation directTableOperation;

    public BroadcastTableModify(DirectTableOperation directTableOperation) {
        super(directTableOperation.getCluster(), directTableOperation.getTraitSet());
        this.directTableOperation = directTableOperation;
    }

    public List<RelNode> getInputs(ExecutionContext executionContext) {
        List<String> groupNames = HintUtil.allGroup(directTableOperation.getSchemaName());

        // May use jingwei to sync broadcast table
        boolean enableBroadcast =
            executionContext.getParamManager().getBoolean(ConnectionParams.CHOOSE_BROADCAST_WRITE);

        if (!enableBroadcast) {
            groupNames = groupNames.subList(0, 1);
        }

        List<RelNode> inputs = new ArrayList<>(groupNames.size());
        for (String groupName : groupNames) {
            DirectTableOperation operation = new DirectTableOperation(directTableOperation);
            operation.setDbIndex(groupName);
            inputs.add(operation);
        }
        return inputs;
    }

    public DirectTableOperation getDirectTableOperation() {
        return directTableOperation;
    }

    @Override
    public String getSchemaName() {
        return directTableOperation.getSchemaName();
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        return directTableOperation.explainTermsForDisplay(pw);
    }
}

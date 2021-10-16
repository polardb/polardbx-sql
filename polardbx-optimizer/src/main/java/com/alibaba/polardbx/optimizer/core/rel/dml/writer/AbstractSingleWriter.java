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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.optimizer.core.rel.dml.SingleWriter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.TableModify.Operation;

/**
 * @author chenmo.cm
 */
public abstract class AbstractSingleWriter extends AbstractWriter implements SingleWriter {
    private final RelOptTable targetTable;

    protected AbstractSingleWriter(RelOptTable targetTable, Operation operation) {
        super(operation);
        this.targetTable = targetTable;
    }

    @Override
    public RelOptTable getTargetTable() {
        return targetTable;
    }

}

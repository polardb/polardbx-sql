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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.ddl.ChangeConsensusRole;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDdl;

/**
 * @author moyi
 * @since 2021/03
 */
public class LogicalChangeConsensusLeader extends BaseDdlOperation {

    public LogicalChangeConsensusLeader(RelOptCluster cluster, RelTraitSet traitSet, SqlDdl sqlDdl,
                                        RelDataType rowType) {
        super(cluster, traitSet, sqlDdl, rowType);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public static LogicalChangeConsensusLeader create(ChangeConsensusRole input) {
        return new LogicalChangeConsensusLeader(input.getCluster(), input.getTraitSet(), input.getAst(),
            input.getRowType());
    }
}

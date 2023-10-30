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

package com.alibaba.polardbx.optimizer.core.rel.dal;

import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import org.apache.calcite.rel.ddl.GenericDdl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlRebalance;

public class LogicalRebalance extends BaseDdlOperation {

    private SqlNode sqlNode;

    public LogicalRebalance(GenericDdl genericDdl, SqlRebalance sqlNode) {
        super(genericDdl.getCluster(), genericDdl.getTraitSet(), genericDdl);
        this.sqlNode = sqlNode;
    }

    public static LogicalRebalance create(GenericDdl genericDdl, SqlRebalance sqlNode) {
        return new LogicalRebalance(genericDdl, sqlNode);
    }

    @Override
    protected String getExplainName() {
        return "LogicalRebalance";
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }
}



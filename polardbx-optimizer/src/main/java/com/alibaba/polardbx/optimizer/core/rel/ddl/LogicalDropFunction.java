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

import org.apache.calcite.rel.ddl.DropFunction;
import org.apache.calcite.sql.SqlDropFunction;

public class LogicalDropFunction extends BaseDdlOperation {
    private SqlDropFunction sqlDropFunction;

    public LogicalDropFunction(DropFunction dropFunction) {
        super(dropFunction);
        this.sqlDropFunction = (SqlDropFunction) relDdl.sqlNode;
    }

    public static LogicalDropFunction create(DropFunction dropFunction) {
        return new LogicalDropFunction(dropFunction);
    }

    public SqlDropFunction getSqlDropFunction() {
        return sqlDropFunction;
    }
}

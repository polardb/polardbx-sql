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

import org.apache.calcite.rel.ddl.CreateFunction;
import org.apache.calcite.sql.SqlCreateFunction;

public class LogicalCreateFunction extends BaseDdlOperation {

    private SqlCreateFunction sqlCreateFunction;

    public LogicalCreateFunction(CreateFunction createfunction) {
        super(createfunction);
        this.sqlCreateFunction = (SqlCreateFunction) relDdl.sqlNode;
    }

    public static LogicalCreateFunction create(CreateFunction createfunction) {
        return new LogicalCreateFunction(createfunction);
    }

    public SqlCreateFunction getSqlCreateFunction() {
        return sqlCreateFunction;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }
}

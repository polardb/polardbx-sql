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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.ddl.TruncateTable;

/**
 * @author lijiu.lzw
 */
public class LogicalInsertOverwrite extends LogicalTruncateTable {
    private String insertSql;
    public LogicalInsertOverwrite(TruncateTable truncateTable) {
        super(truncateTable);
        this.insertSql = null;
    }

    public static LogicalInsertOverwrite create(TruncateTable truncateTable) {
        return new LogicalInsertOverwrite(truncateTable);
    }

    public String getInsertSql() {
        return insertSql;
    }

    public void setInsertSql(String sql) {
        this.insertSql = sql;
    }

    @Override
    public void prepareData(ExecutionContext ec) {
        super.prepareData(ec);
    }
}

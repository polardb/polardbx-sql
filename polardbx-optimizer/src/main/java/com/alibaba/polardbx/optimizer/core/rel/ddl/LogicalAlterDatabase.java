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

import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import org.apache.calcite.rel.ddl.AlterDatabase;
import org.apache.calcite.sql.SqlAlterDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterDatabase extends BaseDdlOperation {
    protected SqlAlterDatabase sqlAlterDatabase;

    public LogicalAlterDatabase(AlterDatabase alterDatabase) {
        super(alterDatabase.getCluster(), alterDatabase.getTraitSet(), alterDatabase);
        this.sqlAlterDatabase = (SqlAlterDatabase) alterDatabase.sqlNode;
        this.setSchemaName(DefaultDbSchema.NAME);
        this.setTableName("nonsense");
        this.relDdl.setTableName(new SqlIdentifier("nonsense", SqlParserPos.ZERO));
    }

    public static LogicalAlterDatabase create(AlterDatabase alterDatabase) {
        return new LogicalAlterDatabase(alterDatabase);
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

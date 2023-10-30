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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateDatabasePreparedData;
import org.apache.calcite.rel.ddl.CreateDatabase;
import org.apache.calcite.sql.SqlCreateDatabase;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class LogicalCreateDatabase extends BaseDdlOperation {
    protected SqlCreateDatabase sqlCreateDatabase;
    protected CreateDatabasePreparedData createDatabasePreparedData;

    public LogicalCreateDatabase(CreateDatabase createDatabase) {
        super(createDatabase.getCluster(), createDatabase.getTraitSet(), createDatabase);
        if (createDatabase.getSqlNode() instanceof SqlCreateDatabase) {
            SqlCreateDatabase sqlCreateDatabase = (SqlCreateDatabase) createDatabase.getSqlNode();
            SqlIdentifier sourceDatabaseName = (SqlIdentifier) sqlCreateDatabase.getSourceDatabaseName();
            if (sqlCreateDatabase.getLike() || sqlCreateDatabase.getAs()) {
                this.setSchemaName(DefaultDbSchema.NAME);
            }
        }
        this.relDdl.setTableName(new SqlIdentifier("nonsense", SqlParserPos.ZERO));
        this.setTableName("nonsense");
        this.sqlCreateDatabase = (SqlCreateDatabase) relDdl.sqlNode;
    }

    public static LogicalCreateDatabase create(CreateDatabase createDatabase) {
        return new LogicalCreateDatabase(createDatabase);
    }

    public CreateDatabasePreparedData getCreateDatabasePreparedData() {
        return createDatabasePreparedData;
    }

    public void setCreateDatabasePreparedData(
        CreateDatabasePreparedData createDatabasePreparedData) {
        this.createDatabasePreparedData = createDatabasePreparedData;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public void prepareData() {
        SqlIdentifier sourceDatabaseName = sqlCreateDatabase.getSourceDatabaseName();
        String srcSchemaName = sourceDatabaseName.names.get(0);
        String dstSchemaName = sqlCreateDatabase.getDbName().getSimple();
        List<String> includeTables = new ArrayList<>();
        List<String> excludeTables = new ArrayList<>();
        sqlCreateDatabase.getIncludeTables().forEach(
            sqlIdentifier -> includeTables.add(sqlIdentifier.getLastName())
        );
        sqlCreateDatabase.getExcludeTables().forEach(
            sqlIdentifier -> excludeTables.add(sqlIdentifier.getLastName())
        );
        boolean needDoCreateTables = sqlCreateDatabase.isCreateTables();

        this.createDatabasePreparedData = new CreateDatabasePreparedData(
            srcSchemaName,
            dstSchemaName,
            includeTables,
            excludeTables,
            sqlCreateDatabase.getLike(),
            sqlCreateDatabase.getAs(),
            sqlCreateDatabase.getWithLock(),
            needDoCreateTables
        );
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }
}

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

import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTableGroupPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.CreateTableGroup;
import org.apache.calcite.sql.SqlCreateTableGroup;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalCreateTableGroup extends BaseDdlOperation {

    protected CreateTableGroupPreparedData preparedData;

    public LogicalCreateTableGroup(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public String getTableGroupName() {
        return ((SqlCreateTableGroup) nativeSqlNode).getTableGroupName();
    }

    public boolean isIfNotExists() {
        return ((SqlCreateTableGroup) nativeSqlNode).isIfNotExists();
    }

    public String getLocality() {
        return ((SqlCreateTableGroup) nativeSqlNode).getLocality();
    }

    public String getPartitionBy() {
        SqlNode sqlPartition = ((SqlCreateTableGroup) nativeSqlNode).getSqlPartition();
        if (sqlPartition != null) {
            return sqlPartition.toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    public static LogicalCreateTableGroup create(CreateTableGroup input) {
        return new LogicalCreateTableGroup(input);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        // prevent creating table group like 'oss_%'
        return TableGroupNameUtil.isOssTg(getTableGroupName());
    }

    public CreateTableGroupPreparedData getPreparedData() {
        if (preparedData == null) {
            preparedData();
        }
        return preparedData;
    }

    public void preparedData() {
        preparedData = new CreateTableGroupPreparedData();
        preparedData.setTableGroupName(getTableGroupName());
        preparedData.setSourceSql("");
        preparedData.setIfNotExists(isIfNotExists());
        preparedData.setSchemaName(getSchemaName());
        preparedData.setLocality(getLocality());
        preparedData.setPartitionBy(getPartitionBy());
    }
}

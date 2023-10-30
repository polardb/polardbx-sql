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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropTableGroupPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.DropTableGroup;
import org.apache.calcite.sql.SqlDropTableGroup;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class LogicalDropTableGroup extends BaseDdlOperation {

    DropTableGroupPreparedData preparedData;

    public LogicalDropTableGroup(DDL ddl) {
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
        return ((SqlDropTableGroup) nativeSqlNode).getTableGroupName();
    }

    public boolean isIfExists() {
        return ((SqlDropTableGroup) nativeSqlNode).isIfExists();
    }

    public static LogicalDropTableGroup create(DropTableGroup input) {
        return new LogicalDropTableGroup(input);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        return TableGroupNameUtil.isOssTg(getTableGroupName());
    }

    public DropTableGroupPreparedData getPreparedData() {
        if (preparedData == null) {
            preparedData();
        }
        return preparedData;
    }

    public void preparedData() {
        preparedData = new DropTableGroupPreparedData();
        preparedData.setTableGroupName(getTableGroupName());
        preparedData.setSourceSql("");
        preparedData.setIfExists(isIfExists());
        preparedData.setSchemaName(getSchemaName());
    }
}

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

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterStoragePoolPrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateStoragePoolPrepareData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterStoragePool;
import org.apache.calcite.rel.ddl.CreateStoragePool;

import java.util.Arrays;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class LogicalAlterStoragePool extends BaseDdlOperation {
    private AlterStoragePool alterStoragePool;

    public AlterStoragePoolPrepareData getPreparedData() {
        return preparedData;
    }

    private AlterStoragePoolPrepareData preparedData;

    public LogicalAlterStoragePool(DDL ddl) {
        super(ddl);
        this.tableName = ddl.getTableName().toString();
        this.alterStoragePool = (AlterStoragePool) ddl;
    }

    public static LogicalAlterStoragePool create(DDL ddl) {
        return new LogicalAlterStoragePool(ddl);
    }

    public void prepareData(Boolean validateStorageInstIdle) {
        AlterStoragePoolPrepareData preparedData = new AlterStoragePoolPrepareData();
        preparedData.setStoragePoolName(alterStoragePool.getStoragePoolName().toString().toLowerCase());
        preparedData.setSchemaName(DEFAULT_DB_NAME);
        String dnList = formatDnList(alterStoragePool.getDnList().toString().toLowerCase());
        String[] dnIds = dnList.split(",");
        preparedData.setDnIds(Arrays.stream(dnIds).collect(Collectors.toList()));
        preparedData.setOperationType(alterStoragePool.getOperationType().toString());
        preparedData.setValidateStorageInstIdle(validateStorageInstIdle);
        this.preparedData = preparedData;
    }

    public String formatDnList(String dnList) {
        return dnList.replaceAll("^\"|\"$|^'|'$", "");
    }

}

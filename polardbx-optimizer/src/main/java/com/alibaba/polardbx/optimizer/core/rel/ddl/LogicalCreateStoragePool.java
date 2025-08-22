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
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateStoragePoolPrepareData;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.CreateFileStorage;
import org.apache.calcite.rel.ddl.CreateStoragePool;
import org.apache.calcite.sql.SqlNode;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class LogicalCreateStoragePool extends BaseDdlOperation {
    private CreateStoragePool createStoragePool;

    public CreateStoragePoolPrepareData getPreparedData() {
        return preparedData;
    }

    private CreateStoragePoolPrepareData preparedData;

    public LogicalCreateStoragePool(DDL ddl) {
        super(ddl);
        this.tableName = ddl.getTableName().toString();
        this.createStoragePool = (CreateStoragePool) ddl;
    }

    public static LogicalCreateStoragePool create(DDL ddl) {
        return new LogicalCreateStoragePool(ddl);
    }

    public void prepareData() {
        CreateStoragePoolPrepareData preparedData = new CreateStoragePoolPrepareData();
        preparedData.setStoragePoolName(createStoragePool.getStoragePoolName().toString().toLowerCase());
        preparedData.setSchemaName(DEFAULT_DB_NAME);
        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
        if (storagePoolManager.storagePoolCacheByName.containsKey(preparedData.getStoragePoolName())) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("duplicate storage pool name '%s' found! ", preparedData.getStoragePoolName()));
        }
        if (createStoragePool.getDnList() != null) {
            String dnList = createStoragePool.getDnList().toString().toLowerCase().replaceAll("^\"|\"$|^'|'$", "");
            String[] dnIds = dnList.split(",");
            preparedData.setDnIds(Arrays.stream(dnIds).collect(Collectors.toList()));
            String undeletableDnId = dnIds[0];
            if (createStoragePool.getUndeletableDn() != null) {
                undeletableDnId = createStoragePool.getUndeletableDn().toString().replaceAll("^\"|\"$|^'|'$", "");
            }
            preparedData.setUndeletableDnId(undeletableDnId);
        }
        this.preparedData = preparedData;
    }

}

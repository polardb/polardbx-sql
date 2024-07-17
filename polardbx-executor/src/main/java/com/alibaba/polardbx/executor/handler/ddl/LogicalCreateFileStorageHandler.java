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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.factory.CreateFileStorageJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.engine.FileStorageInfoKey;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFileStorage;
import org.apache.calcite.rel.ddl.CreateFileStorage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class LogicalCreateFileStorageHandler extends LogicalCommonDdlHandler {
    public LogicalCreateFileStorageHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalCreateFileStorage logicalCreateFileStorage = (LogicalCreateFileStorage) logicalDdlPlan;
        CreateFileStorage createFileStorage = logicalCreateFileStorage.getCreateFileStorage();

        Engine engine = Engine.of(createFileStorage.getEngineName());
        if (engine == null || !Engine.isFileStore(engine)) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "invalid engine : " + createFileStorage.getEngineName());
        }

        if (createFileStorage.isIfNotExists()) {
            try {
                if (FileSystemManager.getInstance().getCache().get(engine).isPresent()) {
                    return new TransientDdlJob();
                }
            } catch (ExecutionException e) {
                //ignore
            }
        }

        // check fileStorageInfoKey
        Map<FileStorageInfoKey, String> with = new HashMap<>();
        Map<FileStorageInfoKey.AzureConnectionStringKey, String> azureSettings = null;
        for (Map.Entry<String, String> e : createFileStorage.getWith().entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            FileStorageInfoKey fileStorageInfoKey;
            if ((fileStorageInfoKey = FileStorageInfoKey.of(key)) == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS, "error key: " + key);
            }
            with.put(fileStorageInfoKey, value);
            if (fileStorageInfoKey == FileStorageInfoKey.AZURE_CONNECTION_STRING) {
                azureSettings = parseConnectionString(value);
            }
        }

        return new CreateFileStorageJobFactory(engine, with, azureSettings, executionContext).create();
    }

    private static Map<FileStorageInfoKey.AzureConnectionStringKey, String> parseConnectionString(
        String connectionString) {
        Map<FileStorageInfoKey.AzureConnectionStringKey, String> parts = new HashMap<>();
        // Split the connection string on semicolons
        String[] pairs = connectionString.split(";");
        for (String pair : pairs) {
            // Split each pair on the first equals sign to separate key and value
            int idx = pair.indexOf('=');
            if (idx > 0 && idx < pair.length() - 1) {
                // Ensure that the key and value are non-empty
                String key = pair.substring(0, idx);
                String value = pair.substring(idx + 1);
                FileStorageInfoKey.AzureConnectionStringKey azureKey;
                if ((azureKey = FileStorageInfoKey.AzureConnectionStringKey.of(key)) == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_OSS,
                        "error key of connection string: " + key);
                }
                parts.put(azureKey, value);
            }
        }
        return parts;
    }
}

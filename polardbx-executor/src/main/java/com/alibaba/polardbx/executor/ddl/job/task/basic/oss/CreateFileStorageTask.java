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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.engine.CachePolicy;
import com.alibaba.polardbx.gms.engine.DeletePolicy;
import com.alibaba.polardbx.gms.engine.FileStorageInfoAccessor;
import com.alibaba.polardbx.gms.engine.FileStorageInfoKey;
import com.alibaba.polardbx.gms.engine.FileStorageInfoRecord;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.ABS_URI_SUFFIX_PATTERN;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.AZURE_WASBS_SCHEME;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.AZURE_WASB_SCHEME;

@Getter
@TaskName(name = "CreateFileStorageTask")
public class CreateFileStorageTask extends BaseGmsTask {
    private String engineName;
    private Map<FileStorageInfoKey, String> items;
    private Map<FileStorageInfoKey.AzureConnectionStringKey, String> azureItems;

    @JSONCreator
    public CreateFileStorageTask(String schemaName, String engineName, Map<FileStorageInfoKey, String> items,
                                 Map<FileStorageInfoKey.AzureConnectionStringKey, String> azureItems) {
        super(schemaName, null);
        this.engineName = engineName;
        this.items = items;
        this.azureItems = azureItems;
        onExceptionTryRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FileStorageInfoAccessor fileStorageInfoAccessor = new FileStorageInfoAccessor();
        fileStorageInfoAccessor.setConnection(metaDbConnection);

        FileStorageInfoRecord record1 = new FileStorageInfoRecord();

        record1.instId = "";
        record1.engine = engineName;
        record1.fileSystemConf = "";
        record1.priority = 1;
        record1.regionId = "";
        record1.availableZoneId = "";
        record1.cachePolicy = CachePolicy.DATA_CACHE.getValue();
        record1.deletePolicy = DeletePolicy.MASTER_ONLY.getValue();
        record1.status = 1;
        String uri = items.getOrDefault(FileStorageInfoKey.FILE_URI, "").trim();
        if (!uri.endsWith("/")) {
            uri = uri + "/";
        }
        record1.fileUri = uri;
        if (Engine.OSS.name().equalsIgnoreCase(record1.engine)) {
            record1.externalEndpoint = items.get(FileStorageInfoKey.ENDPOINT);
            record1.internalClassicEndpoint = items.get(FileStorageInfoKey.ENDPOINT);
            record1.internalVpcEndpoint = items.get(FileStorageInfoKey.ENDPOINT);
            record1.accessKeyId = items.get(FileStorageInfoKey.ACCESS_KEY_ID);
            record1.accessKeySecret = PasswdUtil.encrypt(items.get(FileStorageInfoKey.ACCESS_KEY_SECRET));
        } else if (Engine.S3.name().equalsIgnoreCase(record1.engine)) {
            record1.accessKeyId = items.get(FileStorageInfoKey.ACCESS_KEY_ID);
            record1.accessKeySecret = PasswdUtil.encrypt(items.get(FileStorageInfoKey.ACCESS_KEY_SECRET));
        } else if (Engine.ABS.name().equalsIgnoreCase(record1.engine)) {
            record1.accessKeyId = azureItems.get(FileStorageInfoKey.AzureConnectionStringKey.AccountName);
            record1.accessKeySecret =
                PasswdUtil.encrypt(azureItems.get(FileStorageInfoKey.AzureConnectionStringKey.AccountKey));
            record1.externalEndpoint = azureItems.get(FileStorageInfoKey.AzureConnectionStringKey.EndpointSuffix);
            record1.internalClassicEndpoint =
                azureItems.get(FileStorageInfoKey.AzureConnectionStringKey.EndpointSuffix);
            record1.internalVpcEndpoint = azureItems.get(FileStorageInfoKey.AzureConnectionStringKey.EndpointSuffix);
            String endpointsProtocol =
                azureItems.get(FileStorageInfoKey.AzureConnectionStringKey.DefaultEndpointsProtocol);
            record1.fileUri =
                ("https".equalsIgnoreCase(endpointsProtocol) ? AZURE_WASBS_SCHEME : AZURE_WASB_SCHEME) + "://"
                    + items.get(FileStorageInfoKey.AZURE_CONTAINER_NAME)
                    + "@" + String.format(ABS_URI_SUFFIX_PATTERN, record1.accessKeyId, record1.externalEndpoint) + "/";
        }

        if (fileStorageInfoAccessor.query(Engine.of(engineName)).size() != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_FILE_STORAGE_EXISTS,
                String.format("FileStorage %s already exists", engineName));
        }

        // check the endpoint is right
        int wait = 10;
        List<String> unexpectedErrors = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future future = null;
        try (FileSystem master = FileSystemManager.buildFileSystem(record1)) {
            future = executor.submit(() -> {
                try {
                    master.exists(FileSystemUtils.buildPath(master, "1.orc", false));
                } catch (Exception e) {
                    unexpectedErrors.add(e.getMessage());
                }
            });
            future.get(wait, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            // check the endpoint is right
            throw new TddlRuntimeException(ErrorCode.ERR_OSS_CONNECT,
                "Bad Endpoint value! Failed to connect to oss in " + wait + " seconds!");
        } catch (IOException | InterruptedException | ExecutionException e) {
            unexpectedErrors.add(e.getMessage());
        } finally {
            if (future != null) {
                future.cancel(true);
            }
            if (!unexpectedErrors.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_OSS_CONNECT, unexpectedErrors.get(0));
            }
        }

        fileStorageInfoAccessor.insertIgnore(ImmutableList.of(record1));

        ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
        configListenerAccessor.setConnection(metaDbConnection);
        configListenerAccessor.updateOpVersion(MetaDbDataIdBuilder.getFileStorageInfoDataId());
    }

    protected void updateTableVersion(Connection metaDbConnection) {
    }
}

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
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
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Getter
@TaskName(name = "CreateFileStorageTask")
public class CreateFileStorageTask extends BaseGmsTask {
    private String engineName;
    private Map<FileStorageInfoKey, String> items;

    @JSONCreator
    public CreateFileStorageTask(String schemaName, String engineName, Map<FileStorageInfoKey, String> items) {
        super(schemaName, null);
        this.engineName = engineName;
        this.items = items;
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
        record1.cachePolicy = CachePolicy.META_AND_DATA_CACHE.getValue();
        record1.deletePolicy = DeletePolicy.MASTER_ONLY.getValue();
        record1.status = 1;
        String uri = items.get(FileStorageInfoKey.FILE_URI).trim();
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

            // check the endpoint is right
            int wait = 10;
            List<String> unexpectedErrors = new ArrayList<>();
            ExecutorService executor = Executors.newFixedThreadPool(1);
            Future future = null;
            try (FileSystem master = FileSystemManager.buildFileSystem(record1)) {
                future = executor.submit(() -> {
                    try {
                        master.exists(FileSystemUtils.buildPath(master, "1.orc"));
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
        }

        if (fileStorageInfoAccessor.query(Engine.of(engineName)).size() != 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_FILE_STORAGE_EXISTS, String.format("FileStorage %s already exists", engineName));
        }
        fileStorageInfoAccessor.insertIgnore(ImmutableList.of(record1));

        ConfigListenerAccessor configListenerAccessor = new ConfigListenerAccessor();
        configListenerAccessor.setConnection(metaDbConnection);
        configListenerAccessor.updateOpVersion(MetaDbDataIdBuilder.getFileStorageInfoDataId());
    }

    protected void updateTableVersion(Connection metaDbConnection) {
    }
}

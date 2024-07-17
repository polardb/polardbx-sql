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
import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.engine.FileSystemGroup;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFileStorage;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.ddl.CreateFileStorage;
import org.apache.hadoop.fs.Path;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class LogicalCreateFileStorageHandlerTest {

    final static private String MOCK_AK = "ThisIsAnAk";
    final static private String MOCK_SK = "ThisISASk";
    final private static Map<String, String> MOCK_OSS_WITH = new HashMap<>();
    final private static Map<String, String> MOCK_S3_WITH = new HashMap<>();
    final private static Map<String, String> MOCK_ABS_WITH = new HashMap<>();

    static {
        MOCK_OSS_WITH.put("file_uri", "oss://polardbx-bucket-name/");
        MOCK_OSS_WITH.put("access_key_id", MOCK_AK);
        MOCK_OSS_WITH.put("access_key_secret", PasswdUtil.encrypt(MOCK_SK));
        MOCK_OSS_WITH.put("endpoint", "oss-cn-hangzhou.aliyuncs.com");

        MOCK_S3_WITH.put("file_uri", "s3a://polardbx-bucket-name/");
        MOCK_S3_WITH.put("access_key_id", MOCK_AK);
        MOCK_S3_WITH.put("access_key_secret", PasswdUtil.encrypt(MOCK_SK));

        MOCK_ABS_WITH.put("azure_container_name", "polardbx-container-name");
        MOCK_ABS_WITH.put(
            "azure_connection_string",
            String.format(
                "DefaultEndpointsProtocol=https;AccountName=polardbx-account-name;AccountKey=%s;EndpointSuffix=core.windows.net",
                Base64.getEncoder().encodeToString("polardbx-account-key".getBytes())
            )
        );
    }

    @Mock
    private ExecutionContext executionContext;
    @Mock
    private LogicalCreateFileStorage logicalCreateFileStorage;
    @Mock
    private CreateFileStorage createFileStorage;

    private LogicalCreateFileStorageHandler handler;

    private AutoCloseable mockitoCloseable;

    private MockedStatic<MetaDbUtil> mockedMetaDbUtil;
    private MockedStatic<InstConfUtil> mockedInstConf;
    private MockedStatic<ServerInstIdManager> mockedServerInstId;

    private MockedConstruction<FileMergeCachingFileSystem> fileSystemMockedConstruction;
    private MockedConstruction<DdlEngineTaskAccessor> mockedDdlEngineTaskAccessor;
    private MockedConstruction<FileSystemManager> fileSystemManagerMockedConstruction;

    @Before
    public void setUp() throws SQLException {
        mockitoCloseable = MockitoAnnotations.openMocks(this);
        handler = new LogicalCreateFileStorageHandler(null);

        mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
        Connection mockConnection = Mockito.mock(Connection.class);
        when(MetaDbUtil.getConnection()).thenReturn(mockConnection);
        when(mockConnection.prepareStatement(Mockito.anyString(), Mockito.anyInt())).thenReturn(Mockito.mock(
            PreparedStatement.class));
        mockedInstConf = Mockito.mockStatic(InstConfUtil.class);
        mockedInstConf.when(InstConfUtil::fetchLongConfigs).thenReturn(Maps.newHashMap());
        mockedServerInstId = Mockito.mockStatic(ServerInstIdManager.class);
        mockedServerInstId.when(ServerInstIdManager::getInstance).thenReturn(Mockito.mock(ServerInstIdManager.class));

        fileSystemMockedConstruction = Mockito.mockConstruction(FileMergeCachingFileSystem.class, (mock, context) -> {
            when(mock.exists(Mockito.any())).thenReturn(true);
            when(mock.getWorkingDirectory()).thenReturn(new Path("/"));
        });

        mockedDdlEngineTaskAccessor = Mockito.mockConstruction(DdlEngineTaskAccessor.class, (mock, context) -> {
            when(mock.updateTask(Mockito.any())).thenReturn(1);
        });

        fileSystemManagerMockedConstruction = Mockito.mockConstruction(FileSystemManager.class, (mock, context) -> {
            LoadingCache<Engine, Optional<FileSystemGroup>> mockCache = CacheBuilder.newBuilder().build(
                new CacheLoader<Engine, Optional<FileSystemGroup>>() {
                    @Override
                    public Optional<FileSystemGroup> load(@NotNull Engine key) {
                        if (key == Engine.OSS) {
                            return Optional.of(Mockito.mock(FileSystemGroup.class));
                        }
                        return Optional.empty();
                    }
                });

            when(mock.getCache()).thenReturn(mockCache);
        });
    }

    @After
    public void tearDown() throws Exception {
        if (mockitoCloseable != null) {
            mockitoCloseable.close();
        }

        if (mockedMetaDbUtil != null) {
            mockedMetaDbUtil.close();
        }

        if (mockedInstConf != null) {
            mockedInstConf.close();
        }

        if (mockedServerInstId != null) {
            mockedServerInstId.close();
        }

        if (fileSystemMockedConstruction != null) {
            fileSystemMockedConstruction.close();
        }

        if (mockedDdlEngineTaskAccessor != null) {
            mockedDdlEngineTaskAccessor.close();
        }

        if (fileSystemManagerMockedConstruction != null) {
            fileSystemManagerMockedConstruction.close();
        }
    }

    @Test
    public void testBuildDdlJobWithOss() {
        when(logicalCreateFileStorage.getCreateFileStorage()).thenReturn(createFileStorage);
        when(createFileStorage.getEngineName()).thenReturn("OSS");
        when(createFileStorage.isIfNotExists()).thenReturn(false);
        when(createFileStorage.getWith()).thenReturn(MOCK_OSS_WITH);

        DdlJob ddlJob = handler.buildDdlJob(logicalCreateFileStorage, executionContext);
        DdlTask ddlTask = ddlJob.getAllTasks().get(0);
        prepareDdlTask(ddlTask);
        ddlTask.execute(new ExecutionContext());

        // Test for incomplete settings
        MOCK_OSS_WITH.remove("access_key_secret");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain ACCESS_KEY_SECRET in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_OSS_WITH.remove("access_key_id");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain ACCESS_KEY_ID in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_OSS_WITH.put("endpoint", "invalid_enpoint");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("bad ENDPOINT value in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_OSS_WITH.remove("endpoint");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain ENDPOINT in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_OSS_WITH.remove("file_uri");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain FILE_URI in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );
    }

    @Test
    public void testBuildDdlJobWithS3() {
        when(logicalCreateFileStorage.getCreateFileStorage()).thenReturn(createFileStorage);
        when(createFileStorage.getEngineName()).thenReturn("S3");
        when(createFileStorage.isIfNotExists()).thenReturn(false);
        when(createFileStorage.getWith()).thenReturn(MOCK_S3_WITH);

        DdlJob ddlJob = handler.buildDdlJob(logicalCreateFileStorage, executionContext);
        DdlTask ddlTask = ddlJob.getAllTasks().get(0);
        prepareDdlTask(ddlTask);
        ddlTask.execute(new ExecutionContext());

        // Test for incomplete settings
        MOCK_S3_WITH.remove("access_key_secret");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain ACCESS_KEY_SECRET in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_S3_WITH.remove("access_key_id");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain ACCESS_KEY_ID in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_S3_WITH.remove("file_uri");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain FILE_URI in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );
    }

    @Test
    public void testBuildDdlJobWithAbs() {
        when(logicalCreateFileStorage.getCreateFileStorage()).thenReturn(createFileStorage);
        when(createFileStorage.getEngineName()).thenReturn("ABS");
        when(createFileStorage.isIfNotExists()).thenReturn(false);
        when(createFileStorage.getWith()).thenReturn(MOCK_ABS_WITH);

        DdlJob ddlJob = handler.buildDdlJob(logicalCreateFileStorage, executionContext);
        DdlTask ddlTask = ddlJob.getAllTasks().get(0);
        prepareDdlTask(ddlTask);
        ddlTask.execute(new ExecutionContext());

        // Test for incomplete or invalid settings
        MOCK_ABS_WITH.put("azure_connection_string", String.format(
            "DefaultEndpointsProtocol=https;AccountName=polardbx-account-name;AccountKey=%s;InvalidKey=123",
            Base64.getEncoder().encodeToString("polardbx-account-key".getBytes())
        ));
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("error key of connection string: InvalidKey"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_ABS_WITH.put("azure_connection_string", String.format(
            "DefaultEndpointsProtocol=https;AccountName=polardbx-account-name;AccountKey=%s",
            Base64.getEncoder().encodeToString("polardbx-account-key".getBytes())
        ));
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain EndpointSuffix in connection string!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_ABS_WITH.put("azure_connection_string",
            "DefaultEndpointsProtocol=https;AccountName=polardbx-account-name");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain AccountKey in connection string!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_ABS_WITH.put("azure_connection_string", "DefaultEndpointsProtocol=https");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain AccountName in connection string!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_ABS_WITH.put("azure_connection_string", "");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain DefaultEndpointsProtocol in connection string!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_ABS_WITH.remove("azure_container_name");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain AZURE_CONTAINER_NAME in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );

        MOCK_ABS_WITH.remove("azure_connection_string");
        MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("Should contain AZURE_CONNECTION_STRING in with!"),
            () -> {
                handler.buildDdlJob(logicalCreateFileStorage, executionContext);
            }
        );
    }

    @Test
    public void testBuildDdlJobWithInvalidEngine() {
        when(logicalCreateFileStorage.getCreateFileStorage()).thenReturn(createFileStorage);
        when(createFileStorage.getEngineName()).thenReturn("invalid_engine");

        MockUtils.assertThrows(RuntimeException.class, null, () ->
            handler.buildDdlJob(logicalCreateFileStorage, executionContext));
    }

    @Test
    public void testBuildDdlJobWithIfNotExistsAndExists() {
        when(logicalCreateFileStorage.getCreateFileStorage()).thenReturn(createFileStorage);
        when(createFileStorage.getEngineName()).thenReturn("OSS");
        when(createFileStorage.isIfNotExists()).thenReturn(true);

        // Here, we assume the FileSystemManager returns a present optional indicating the storage already exists.
        DdlJob ddlJob = handler.buildDdlJob(logicalCreateFileStorage, executionContext);
        assertTrue(ddlJob instanceof TransientDdlJob);
    }

    static private void prepareDdlTask(DdlTask ddlTask) {
        ddlTask.setRootJobId(1L);
        ddlTask.setJobId(1L);
        ddlTask.setTaskId(1L);
        ddlTask.setSchemaName("test");
    }
}
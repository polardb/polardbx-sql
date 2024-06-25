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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.common.oss.filesystem.NFSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.engine.decorator.FileSystemDecorator;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.emc.ecs.nfsclient.nfs.nfs3.Nfs3;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azure.AzureNativeFileSystemStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class FileSystemManagerTest {

    final static private FileStorageInfoRecord MOCK_OSS_RECORD = new FileStorageInfoRecord();
    final static private FileStorageInfoRecord MOCK_LOCAL_DISK_RECORD = new FileStorageInfoRecord();
    final static private FileStorageInfoRecord MOCK_EXTERNAL_DISK_RECORD = new FileStorageInfoRecord();
    final static private FileStorageInfoRecord MOCK_NFS_RECORD = new FileStorageInfoRecord();
    final static private FileStorageInfoRecord MOCK_S3_RECORD = new FileStorageInfoRecord();
    final static private FileStorageInfoRecord MOCK_ABS_RECORD = new FileStorageInfoRecord();
    final static private FileStorageInfoRecord MOCK_ABS_HTTPS_RECORD = new FileStorageInfoRecord();
    final static private FileStorageInfoRecord MOCK_INVALID_RECORD = new FileStorageInfoRecord();

    final static private String MOCK_AK = "ThisIsAnAk";
    final static private String MOCK_SK = "ThisISASk";

    private MockedStatic<InstConfUtil> mockedInstConf;
    private MockedStatic<ServerInstIdManager> mockedServerInstId;
    private MockedStatic<MetaDbConfigManager> mockedMetaDbInstConfig;
    private MockedStatic<MetaDbUtil> mockedMetaDbUtil;
    private MockedConstruction<Nfs3> nfs3MockedConstruction;
    private MockedConstruction<AzureNativeFileSystemStore> azureNativeFileSystemStoreMockedConstruction;
    private MockedConstruction<FileStorageInfoAccessor> accessorMockedConstruction;

    static {
        MOCK_OSS_RECORD.engine = Engine.OSS.name();
        MOCK_OSS_RECORD.externalEndpoint
            = MOCK_OSS_RECORD.internalClassicEndpoint
            = MOCK_OSS_RECORD.internalVpcEndpoint
            = "oss-cn-hangzhou.aliyuncs.com";
        MOCK_OSS_RECORD.accessKeyId = MOCK_AK;
        MOCK_OSS_RECORD.accessKeySecret = PasswdUtil.encrypt(MOCK_SK);
        MOCK_OSS_RECORD.fileUri = "oss://polardbx-bucket-name/";

        MOCK_LOCAL_DISK_RECORD.engine = Engine.LOCAL_DISK.name();
        MOCK_LOCAL_DISK_RECORD.fileUri = "file:///test-dir/";

        MOCK_EXTERNAL_DISK_RECORD.engine = Engine.EXTERNAL_DISK.name();
        MOCK_EXTERNAL_DISK_RECORD.fileUri = "file:///test-dir-2/";

        MOCK_NFS_RECORD.engine = Engine.NFS.name();
        MOCK_NFS_RECORD.fileUri = "nfs://127.0.0.1/polardbx-test";

        MOCK_S3_RECORD.engine = Engine.S3.name();
        MOCK_S3_RECORD.fileUri = "s3://polardbx-bucket-name/";
        MOCK_S3_RECORD.accessKeyId = MOCK_AK;
        MOCK_S3_RECORD.accessKeySecret = PasswdUtil.encrypt(MOCK_SK);

        MOCK_ABS_RECORD.engine = Engine.ABS.name();
        MOCK_ABS_RECORD.fileUri = "wasb://polardbx-container-name/";
        MOCK_ABS_RECORD.externalEndpoint = "localhost";
        MOCK_ABS_RECORD.accessKeyId = MOCK_AK;
        MOCK_ABS_RECORD.accessKeySecret = PasswdUtil.encrypt(MOCK_SK);

        MOCK_ABS_HTTPS_RECORD.engine = Engine.ABS.name();
        MOCK_ABS_HTTPS_RECORD.fileUri = "wasbs://polardbx-container-name/";
        MOCK_ABS_HTTPS_RECORD.externalEndpoint = "localhost";
        MOCK_ABS_HTTPS_RECORD.accessKeyId = MOCK_AK;
        MOCK_ABS_HTTPS_RECORD.accessKeySecret = PasswdUtil.encrypt(MOCK_SK);
    }

    @Before
    public void setUp() {
        mockedInstConf = Mockito.mockStatic(InstConfUtil.class);
        mockedServerInstId = Mockito.mockStatic(ServerInstIdManager.class);
        mockedInstConf.when(InstConfUtil::fetchLongConfigs).thenReturn(Maps.newHashMap());
        ServerInstIdManager mockedServerInstIdManager = Mockito.mock(ServerInstIdManager.class);
        mockedServerInstId.when(ServerInstIdManager::getInstance).thenReturn(mockedServerInstIdManager);

        nfs3MockedConstruction = Mockito.mockConstruction(Nfs3.class);

        azureNativeFileSystemStoreMockedConstruction = Mockito.mockConstruction(AzureNativeFileSystemStore.class);

        mockedMetaDbInstConfig = Mockito.mockStatic(MetaDbConfigManager.class);
        mockedMetaDbInstConfig.when(MetaDbConfigManager::getInstance).thenReturn(Mockito.mock(
            MetaDbConfigManager.class));

        mockedMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);

        accessorMockedConstruction = Mockito.mockConstruction(FileStorageInfoAccessor.class, (mock, context) -> {
            Mockito.when(mock.query(Mockito.any(Engine.class))).thenAnswer(invocation -> {
                Engine engine = invocation.getArgument(0);
                if (engine == Engine.OSS) {
                    return Lists.newArrayList(MOCK_OSS_RECORD);
                } else if (engine == Engine.LOCAL_DISK) {
                    return Lists.newArrayList(MOCK_LOCAL_DISK_RECORD);
                } else if (engine == Engine.EXTERNAL_DISK) {
                    return Lists.newArrayList(MOCK_EXTERNAL_DISK_RECORD);
                } else if (engine == Engine.NFS) {
                    return Lists.newArrayList(MOCK_NFS_RECORD);
                } else if (engine == Engine.S3) {
                    return Lists.newArrayList(MOCK_S3_RECORD);
                } else if (engine == Engine.ABS) {
                    return Lists.newArrayList(MOCK_ABS_RECORD);
                }
                return null;
            });
        });
    }

    @After
    public void clear() {
        if (mockedInstConf != null) {
            mockedInstConf.close();
        }

        if (mockedServerInstId != null) {
            mockedServerInstId.close();
        }

        if (nfs3MockedConstruction != null) {
            nfs3MockedConstruction.close();
        }

        if (azureNativeFileSystemStoreMockedConstruction != null) {
            azureNativeFileSystemStoreMockedConstruction.close();
        }

        if (mockedMetaDbInstConfig != null) {
            mockedMetaDbInstConfig.close();
        }

        if (mockedMetaDbUtil != null) {
            mockedMetaDbUtil.close();
        }

        if (accessorMockedConstruction != null) {
            accessorMockedConstruction.close();
        }
    }

    @Test
    public void buildFileSystem() throws IOException {
        FileSystem fs = FileSystemManager.buildFileSystem(MOCK_OSS_RECORD);
        Assert.assertTrue(fs instanceof FileMergeCachingFileSystem);
        FileSystem dataTier = ((FileMergeCachingFileSystem) fs).getDataTier();
        Assert.assertTrue(dataTier instanceof OSSFileSystem);
        Assert.assertEquals("oss", dataTier.getScheme());
        fs.close();

        fs = FileSystemManager.buildFileSystem(MOCK_LOCAL_DISK_RECORD);
        Assert.assertEquals("file", fs.getScheme());
        Assert.assertFalse(fs instanceof FileMergeCachingFileSystem);
        fs.close();

        fs = FileSystemManager.buildFileSystem(MOCK_EXTERNAL_DISK_RECORD);
        Assert.assertEquals("file", fs.getScheme());
        Assert.assertTrue(fs instanceof FileMergeCachingFileSystem);
        fs.close();

        fs = FileSystemManager.buildFileSystem(MOCK_NFS_RECORD);
        Assert.assertTrue(fs instanceof FileMergeCachingFileSystem);
        dataTier = ((FileMergeCachingFileSystem) fs).getDataTier();
        Assert.assertTrue(dataTier instanceof NFSFileSystem);
        Assert.assertEquals("nfs", dataTier.getScheme());
        fs.close();

        fs = FileSystemManager.buildFileSystem(MOCK_S3_RECORD);
        Assert.assertTrue(fs instanceof FileMergeCachingFileSystem);
        dataTier = ((FileMergeCachingFileSystem) fs).getDataTier();
        Assert.assertTrue(dataTier instanceof FileSystemDecorator);
        Assert.assertEquals("s3a", dataTier.getScheme());
        fs.close();

        fs = FileSystemManager.buildFileSystem(MOCK_ABS_RECORD);
        Assert.assertTrue(fs instanceof FileMergeCachingFileSystem);
        dataTier = ((FileMergeCachingFileSystem) fs).getDataTier();
        Assert.assertTrue(dataTier instanceof FileSystemDecorator);
        Assert.assertEquals("wasb", dataTier.getScheme());
        fs.close();

        fs = FileSystemManager.buildFileSystem(MOCK_ABS_HTTPS_RECORD);
        Assert.assertTrue(fs instanceof FileMergeCachingFileSystem);
        dataTier = ((FileMergeCachingFileSystem) fs).getDataTier();
        Assert.assertTrue(dataTier instanceof FileSystemDecorator);
        Assert.assertEquals("wasbs", dataTier.getScheme());
        fs.close();

        TddlRuntimeException exception = MockUtils.assertThrows(
            TddlRuntimeException.class,
            ErrorCode.ERR_EXECUTE_ON_OSS.getMessage("bad engine = INNODB"),
            () -> {
                FileSystem tmpFs = FileSystemManager.buildFileSystem(MOCK_INVALID_RECORD);
                tmpFs.close();
            }
        );

        Assert.assertEquals(ErrorCode.ERR_EXECUTE_ON_OSS.getCode(), exception.getErrorCode());
    }

    @Test
    public void resetRate() {
        FileSystemManager.resetRate();
    }
}
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
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.GuavaFileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.GuavaFileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.time.parser.StringNumericParser;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;

public class FileSystemUtils {
    public static final String DELIMITER = "/";
    private static final File LOCAL_DIRECTORY = new File("/tmp/");

    public static String buildUri(FileSystem fileSystem, String fileName) {
        return String.join(DELIMITER, fileSystem.getWorkingDirectory().toString(), fileName);
    }

    public static String buildColumnarUri(FileSystem fileSystem, String fileName) {
        if (fileSystem instanceof CachingFileSystem) {
            FileSystem dataFileSystem = ((CachingFileSystem) fileSystem).getDataTier();
            if (dataFileSystem instanceof OSSFileSystem) {
                // columnar_oss_directory only works for OSS engine
                return String.join(DELIMITER, fileSystem.getUri().toString(), getColumnarDirectory(), fileName);
            }
        } else if (fileSystem instanceof OSSFileSystem) {
            // columnar_oss_directory only works for OSS engine
            return String.join(DELIMITER, fileSystem.getUri().toString(), getColumnarDirectory(), fileName);
        }
        return String.join(DELIMITER, fileSystem.getWorkingDirectory().toString(), fileName);
    }

    // keep API compatible
    // this API is deprecated by CN, but is still used by columnar node
    @Deprecated
    public static Path buildPath(FileSystem fileSystem, String fileName) {
        return buildPath(fileSystem, fileName, false);
    }

    public static Path buildPath(FileSystem fileSystem, String fileName, boolean isColumnar) {
        if (isColumnar) {
            return new Path(buildColumnarUri(fileSystem, fileName));
        } else {
            return new Path(buildUri(fileSystem, fileName));
        }
    }

    public static void writeFile(File localFile, String fileName, Engine engine, boolean isColumnar)
        throws IOException {
        FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine);
        FileSystem fileSystem = fileSystemGroup.getMaster();
        fileSystemGroup.writeFile(localFile, buildPath(fileSystem, fileName, isColumnar));
    }

    public static boolean deleteIfExistsFile(String fileName, Engine engine, boolean isColumnar) {
        try {
            FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine);
            return fileSystemGroup.delete(fileName, false, isColumnar);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static void readFile(String fileName, OutputStream outputStream, Engine engine, boolean isColumnar) {
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
        try (InputStream in = fileSystem.open(buildPath(fileSystem, fileName, isColumnar))) {
            IOUtils.copy(in, outputStream);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static boolean fileExists(String fileName, Engine engine, boolean isColumnar) {
        try {
            FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
            return fileSystem.exists(buildPath(fileSystem, fileName, isColumnar));
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Read fully file from engine.
     */
    @Deprecated // bad performance for OSS
    public static byte[] readFullyFile(String fileName, Engine engine, boolean isColumnar) {
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
        Path path = buildPath(fileSystem, fileName, isColumnar);
        try (InputStream in = fileSystem.open(path)) {
            return IOUtils.toByteArray(in);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Read file from the offset
     */
    public static void readFile(String fileName, int offset, int length, byte[] output, Engine engine,
                                boolean isColumnar) {
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
        try (InputStream in = fileSystem.open(buildPath(fileSystem, fileName, isColumnar))) {
            ((PositionedReadable) in).readFully(offset, output, 0, length);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static FSDataInputStream openStreamFileWithBuffer(String fileName, Engine engine, boolean isColumnar) {
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
        Path filePath = buildPath(fileSystem, fileName, isColumnar);
        try {
            if (engine == Engine.OSS) {
                // This will bypass cache filesystem for OSS
                OSSFileSystem ossFileSystem = (OSSFileSystem) ((CachingFileSystem) fileSystem).getDataTier();
                return ossFileSystem.open(filePath);
            } else {
                return fileSystem.open(filePath);
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static FSDataInputStream openStreamFile(String fileName, Engine engine, boolean isColumnar) {
        FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
        Path filePath = buildPath(fileSystem, fileName, isColumnar);
        try {
            return fileSystem.open(filePath);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static File createLocalFile(String fileName) {
        File localFile = new File(fileName);
        if (localFile.exists()) {
            localFile.delete();
        }
        return localFile;
    }

    public static ColdDataStatus getColdDataStatus() {
        final int status = Integer.parseInt(
            MetaDbInstConfigManager.getInstance().getInstProperty(
                ConnectionProperties.COLD_DATA_STATUS,
                ConnectionParams.COLD_DATA_STATUS.getDefault()
            )
        );
        return ColdDataStatus.of(status);
    }

    private static String getColumnarDirectory() {
        String columnarDirectory = DynamicConfig.getInstance().getColumnarOssDirectory();
        return StringUtils.isEmpty(columnarDirectory) ? ServerInstIdManager.getInstance().getMasterInstId() :
            columnarDirectory;
    }

    private static String getColdDataDirectory() {
        return ServerInstIdManager.getInstance().getMasterInstId();
    }

    public static String getSuffix(String fileName) {
        return fileName.substring(fileName.lastIndexOf('.') + 1);
    }

    public static ColumnarFileType getFileType(String fileName) {
        String suffix = getSuffix(fileName);
        return ColumnarFileType.of(suffix);
    }

    public static GuavaFileSystemRateLimiter newRateLimiter() {
        // fetch rate params
        Map<String, Long> globalVariables = InstConfUtil.fetchLongConfigs(
            ConnectionParams.OSS_FS_MAX_READ_RATE,
            ConnectionParams.OSS_FS_MAX_WRITE_RATE
        );
        Long maxReadRate = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_MAX_READ_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_READ_RATE.getDefault()));
        Long maxWriteRate = Optional.ofNullable(globalVariables.get(ConnectionProperties.OSS_FS_MAX_WRITE_RATE))
            .orElse(StringNumericParser.simplyParseLong(ConnectionParams.OSS_FS_MAX_WRITE_RATE.getDefault()));

        return new GuavaFileSystemRateLimiter(
            maxReadRate == null ? -1 : maxReadRate,
            maxWriteRate == null ? -1 : maxWriteRate
        );
    }
}

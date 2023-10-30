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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.aliyun.oss.event.ProgressListener;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileSystemUtils {

    public static final String DELIMITER = "/";
    private static final File LOCAL_DIRECTORY = new File("/tmp/");

    public static String buildUri(FileSystem fileSystem, String fileName) {
        return String.join(DELIMITER, fileSystem.getWorkingDirectory().toString(), fileName);
    }

    public static Path buildPath(FileSystem fileSystem, String fileName) {
        return new Path(buildUri(fileSystem, fileName));
    }

    public static void writeFile(File localFile, String ossKey, Engine engine) throws IOException {
        long stamp = FileSystemManager.readLockWithTimeOut(engine);
        try {
            FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine);
            fileSystemGroup.writeFile(localFile, ossKey);
        } finally {
            FileSystemManager.unlockRead(engine, stamp);
        }
    }

    public static void writeFile(File localFile, String ossKey, ProgressListener progressListener, Engine engine)
        throws IOException {
        long stamp = FileSystemManager.readLockWithTimeOut(engine);
        try {
            FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine);
            fileSystemGroup.writeFile(localFile, ossKey);
        } finally {
            FileSystemManager.unlockRead(engine, stamp);
        }
    }

    public static boolean deleteIfExistsFile(String ossKey, Engine engine) {
        long stamp = FileSystemManager.readLockWithTimeOut(engine);
        try {
            FileSystemGroup fileSystemGroup = FileSystemManager.getFileSystemGroup(engine);
            return fileSystemGroup.delete(ossKey, false);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        } finally {
            FileSystemManager.unlockRead(engine, stamp);
        }
    }

    public static void readFile(String ossKey, OutputStream outputStream, Engine engine) {
        long stamp = FileSystemManager.readLockWithTimeOut(engine);
        try {
            FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
            try (InputStream in = fileSystem.open(buildPath(fileSystem, ossKey))) {
                IOUtils.copy(in, outputStream);
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
        } finally {
            FileSystemManager.unlockRead(engine, stamp);
        }

    }

    /**
     * Read file from the offset
     */
    public static void readFile(String ossKey, int offset, int length, byte[] output, Engine engine) {
        long stamp = FileSystemManager.readLockWithTimeOut(engine);
        try {
            FileSystem fileSystem = FileSystemManager.getFileSystemGroup(engine).getMaster();
            try (InputStream in = fileSystem.open(buildPath(fileSystem, ossKey))) {
                ((Seekable) in).seek(offset);
                IOUtils.read(in, output, 0, length);
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
        } finally {
            FileSystemManager.unlockRead(engine, stamp);
        }
    }

    public static File createLocalFile(String fileName) {
        File localFile = new File(fileName);
        if (localFile.exists()) {
            localFile.delete();
        }
        return localFile;
    }
}

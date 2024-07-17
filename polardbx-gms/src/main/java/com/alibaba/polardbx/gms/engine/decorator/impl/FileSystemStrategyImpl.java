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

package com.alibaba.polardbx.gms.engine.decorator.impl;

import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.InputStreamWithRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.OutputStreamWithRateLimiter;
import com.alibaba.polardbx.gms.engine.decorator.FileSystemStrategy;
import com.alibaba.polardbx.gms.engine.decorator.FileSystemWrapper;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;

public class FileSystemStrategyImpl implements FileSystemStrategy {

    final private Cache<Path, FileStatus> metaCache;
    final private boolean enableCache;
    final private FileSystemRateLimiter rateLimiter;

    public FileSystemStrategyImpl(boolean enableCache, FileSystemRateLimiter rateLimiter) {
        this.enableCache = enableCache;
        this.rateLimiter = rateLimiter;
        this.metaCache = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterAccess(300, SECONDS)
            .build();
    }

    @Override
    public FileStatus getFileStatus(FileSystem fs, Path path) throws IOException {
        final Callable<FileStatus> valueLoader = () -> fs.getFileStatus(path);
        FileStatus fileStatus;
        try {
            if (enableCache) {
                fileStatus = metaCache.get(path, valueLoader);
            } else {
                fileStatus = fs.getFileStatus(path);
            }
        } catch (FileNotFoundException fnfe) {
            throw fnfe;
        } catch (ExecutionException ex) {
            fileStatus = fs.getFileStatus(path);
        }
        return fileStatus;
    }

    @Override
    public FileSystemRateLimiter getRateLimiter() {
        return rateLimiter;
    }

    @Override
    public FSDataInputStream open(FileSystem fs, Path f, int bufferSize) throws IOException {
        return new FSDataInputStream(
            new InputStreamWithRateLimiter(
                fs.open(f, bufferSize),
                rateLimiter
            )
        );
    }

    @Override
    public FSDataOutputStream create(FileSystem fs, Path f, FsPermission permission, boolean overwrite, int bufferSize,
                                     short replication, long blockSize, Progressable progress) throws IOException {
        return new FSDataOutputStream(
            new OutputStreamWithRateLimiter(
                fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress),
                rateLimiter
            ),
            ((FileSystemWrapper) fs).getInnerStatistics()
        );
    }

    @Override
    public FSDataOutputStream append(FileSystem fs, Path f, int bufferSize, Progressable progress) throws IOException {
        return new FSDataOutputStream(
            new OutputStreamWithRateLimiter(
                fs.append(f, bufferSize, progress),
                rateLimiter
            ),
            ((FileSystemWrapper) fs).getInnerStatistics()
        );
    }

    @Override
    public FSDataOutputStream createNonRecursive(FileSystem fs, Path p, FsPermission permission,
                                                 EnumSet<CreateFlag> flags,
                                                 int bufferSize, short replication, long blockSize,
                                                 Progressable progress) throws IOException {
        return new FSDataOutputStream(
            new OutputStreamWithRateLimiter(
                fs.createNonRecursive(p, permission, flags, bufferSize, replication, blockSize, progress),
                rateLimiter
            ),
            ((FileSystemWrapper) fs).getInnerStatistics()
        );
    }
}

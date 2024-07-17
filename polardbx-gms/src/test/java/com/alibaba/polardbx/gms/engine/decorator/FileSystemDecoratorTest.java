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

package com.alibaba.polardbx.gms.engine.decorator;

import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.common.oss.filesystem.GuavaFileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.InputStreamWithRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.OutputStreamWithRateLimiter;
import com.alibaba.polardbx.gms.engine.decorator.impl.FileSystemStrategyImpl;
import com.alibaba.polardbx.gms.engine.decorator.impl.S3AFileSystemWrapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FileSystemDecoratorTest {

    private static final Path MOCK_PATH = new Path("/");

    private FileSystem delegate;
    private FileSystemDecorator fileSystemDecorator;
    private FileSystemDecorator fileSystemDecoratorWithoutCache;
    private FSDataInputStream mockInputStream;
    private FSDataOutputStream mockOutputStream;
    private FileStatus mockFileStatus;

    @Before
    public void setUp() throws IOException {
        // Mock the FileSystemStrategy and FileSystem
        FileSystemStrategy strategy =
            new FileSystemStrategyImpl(true, new GuavaFileSystemRateLimiter(100, 100));
        FileSystemStrategy strategyWithoutCache =
            new FileSystemStrategyImpl(false, new GuavaFileSystemRateLimiter(100, 100));

        delegate = Mockito.mock(S3AFileSystemWrapper.class);

        mockInputStream = Mockito.mock(FSDataInputStream.class);
        mockOutputStream = Mockito.mock(FSDataOutputStream.class);
        mockFileStatus = Mockito.mock(FileStatus.class);

        when(delegate.open(
            any(Path.class), anyInt())
        ).thenReturn(mockInputStream);
        when(delegate.create(
            any(Path.class), any(), anyBoolean(), anyInt(), anyShort(), anyLong(), any())
        ).thenReturn(mockOutputStream);
        when(delegate.append(
            any(Path.class), anyInt(), any())
        ).thenReturn(mockOutputStream);
        when(delegate.getFileStatus(
            any())
        ).thenReturn(mockFileStatus);
        when(delegate.createNonRecursive(
            any(), any(), any(), anyInt(), anyShort(), anyLong(), any())
        ).thenReturn(mockOutputStream);

        // Initialize FileSystemDecorator with mocked strategy and delegate
        fileSystemDecorator = new FileSystemDecorator(delegate, strategy);
        fileSystemDecoratorWithoutCache = new FileSystemDecorator(delegate, strategyWithoutCache);
    }

    @Test
    public void testOpen() throws IOException {
        FSDataInputStream actualInputStream = fileSystemDecorator.open(MOCK_PATH, 4096);

        Assert.assertTrue(actualInputStream.getWrappedStream() instanceof InputStreamWithRateLimiter);
        Assert.assertEquals(
            mockInputStream,
            ((InputStreamWithRateLimiter) actualInputStream.getWrappedStream()).getWrappedStream()
        );
    }

    @Test
    public void testCreate() throws IOException {
        FSDataOutputStream actualOutputStream =
            fileSystemDecorator.create(
                MOCK_PATH, null, true, 4096, (short) 1, 1024, null
            );
        Assert.assertTrue(actualOutputStream.getWrappedStream() instanceof OutputStreamWithRateLimiter);
        Assert.assertEquals(
            mockOutputStream,
            ((OutputStreamWithRateLimiter) actualOutputStream.getWrappedStream()).getWrappedStream()
        );
    }

    @Test
    public void testAppend() throws IOException {
        FSDataOutputStream actualOutputStream =
            fileSystemDecorator.append(MOCK_PATH, 4096, null);
        Assert.assertTrue(actualOutputStream.getWrappedStream() instanceof OutputStreamWithRateLimiter);
        Assert.assertEquals(
            mockOutputStream,
            ((OutputStreamWithRateLimiter) actualOutputStream.getWrappedStream()).getWrappedStream()
        );
    }

    @Test
    public void testGetFileStatus() throws IOException {
        FileStatus actualFileStatus = fileSystemDecorator.getFileStatus(MOCK_PATH);
        Assert.assertEquals(mockFileStatus, actualFileStatus);

        actualFileStatus = fileSystemDecoratorWithoutCache.getFileStatus(MOCK_PATH);
        Assert.assertEquals(mockFileStatus, actualFileStatus);
    }

    @Test
    public void testCreateNonRecursive() throws IOException {
        FSDataOutputStream actualOutputStream = fileSystemDecorator.createNonRecursive(MOCK_PATH, null,
            null, 4096, (short) 1, 1024, null);
        Assert.assertTrue(actualOutputStream.getWrappedStream() instanceof OutputStreamWithRateLimiter);
        Assert.assertEquals(
            mockOutputStream,
            ((OutputStreamWithRateLimiter) actualOutputStream.getWrappedStream()).getWrappedStream()
        );
    }

    private void verifyForward(MockUtils.ThrowableConsumer<FileSystem> consumer) {
        try {
            consumer.accept(fileSystemDecorator);
            consumer.accept(verify(delegate, times(1)));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testForwardingMethods() throws Throwable {
        verifyForward(fs -> fs.rename(MOCK_PATH, MOCK_PATH));
        verifyForward(fs -> fs.delete(MOCK_PATH, true));
        verifyForward(fs -> fs.listStatus(MOCK_PATH));
        verifyForward(FileSystem::getWorkingDirectory);
        verifyForward(fs -> fs.mkdirs(MOCK_PATH, null));
        verifyForward(fs -> fs.makeQualified(MOCK_PATH));
        verifyForward(fs -> fs.createFile(MOCK_PATH));
        verifyForward(fs -> fs.listStatusIterator(MOCK_PATH));
        verifyForward(fs -> fs.getContentSummary(MOCK_PATH));
        verifyForward(fs -> fs.access(MOCK_PATH, null));
        verifyForward(fs -> fs.copyFromLocalFile(false, false, MOCK_PATH, MOCK_PATH));
        verifyForward(fs -> fs.deleteOnExit(MOCK_PATH));
        verifyForward(fs -> fs.cancelDeleteOnExit(MOCK_PATH));
        verifyForward(FileSystem::getCanonicalServiceName);
        verifyForward(FileSystem::getAdditionalTokenIssuers);
        verifyForward(FileSystem::getDefaultBlockSize);
        verifyForward(fs -> fs.globStatus(MOCK_PATH));
        verifyForward(fs -> fs.globStatus(MOCK_PATH, null));
        verifyForward(fs -> fs.exists(MOCK_PATH));
        verifyForward(fs -> fs.isDirectory(MOCK_PATH));
        verifyForward(fs -> fs.isFile(MOCK_PATH));
        verifyForward(fs -> fs.getFileChecksum(MOCK_PATH, 0));
        verifyForward(fs -> fs.getXAttrs(MOCK_PATH));
        verifyForward(fs -> fs.listXAttrs(MOCK_PATH));
        verifyForward(fs -> fs.listFiles(MOCK_PATH, false));
        verifyForward(fs -> fs.listLocatedStatus(MOCK_PATH));
        verifyForward(fs -> fs.hasPathCapability(MOCK_PATH, ""));
        verifyForward(FileSystem::getHomeDirectory);
        verifyForward(fs -> fs.getFileBlockLocations(mockFileStatus, 0, 1));
        verifyForward(fs -> fs.setOwner(MOCK_PATH, "", ""));
        verifyForward(fs -> fs.setXAttr(MOCK_PATH, "", new byte[0], null));
        verifyForward(fs -> fs.getXAttr(MOCK_PATH, ""));
        verifyForward(fs -> fs.setPermission(MOCK_PATH, null));
        verifyForward(Configured::getConf);
    }

}
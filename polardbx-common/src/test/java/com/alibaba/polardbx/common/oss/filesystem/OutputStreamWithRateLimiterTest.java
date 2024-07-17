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

package com.alibaba.polardbx.common.oss.filesystem;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.io.OutputStream;

@RunWith(MockitoJUnitRunner.class)
public class OutputStreamWithRateLimiterTest {
    private OutputStream mockOutputStream;
    private FileSystemRateLimiter mockRateLimiter;
    private OutputStreamWithRateLimiter outputStreamWithRateLimiter;

    @Before
    public void setUp() {
        mockOutputStream = Mockito.mock(OutputStream.class);
        mockRateLimiter = Mockito.mock(FileSystemRateLimiter.class);
        outputStreamWithRateLimiter = new OutputStreamWithRateLimiter(mockOutputStream, mockRateLimiter);
    }

    @Test
    public void testWriteArray() throws IOException {
        byte[] b = new byte[] {1, 2, 3, 4};
        outputStreamWithRateLimiter.write(b);
        Mockito.verify(mockRateLimiter).acquireWrite(b.length);
        Mockito.verify(mockOutputStream).write(b);
    }

    @Test
    public void testWriteArrayWithOffsetAndLength() throws IOException {
        byte[] b = new byte[] {1, 2, 3, 4, 5, 6};
        int off = 2;
        int len = 4;
        outputStreamWithRateLimiter.write(b, off, len);
        Mockito.verify(mockRateLimiter).acquireWrite(len);
        Mockito.verify(mockOutputStream).write(b, off, len);
    }

    @Test
    public void testFlush() throws IOException {
        outputStreamWithRateLimiter.flush();
        Mockito.verify(mockOutputStream).flush();
    }

    @Test
    public void testClose() throws IOException {
        outputStreamWithRateLimiter.close();
        Mockito.verify(mockOutputStream).close();
    }

    @Test
    public void testWriteInt() throws IOException {
        byte b = 1;
        outputStreamWithRateLimiter.write(b);
        Mockito.verify(mockRateLimiter).acquireWrite(1);
        Mockito.verify(mockOutputStream).write(b);
    }
}
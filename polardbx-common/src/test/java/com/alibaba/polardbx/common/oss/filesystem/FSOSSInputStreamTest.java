package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.common.mock.MockUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;

public class FSOSSInputStreamTest {

    private static final String OSS_KEY_PATH = "oss://bucket/path/to/file";
    private static final int BUFFER_SIZE = 8192;
    private static final long TEST_SEEK_POS = 2048;
    private static final int TEST_READ = 3000;
    private static final long TEST_SKIP = 4000;
    private static final long FILE_LENGTH = 10000;
    private FSOSSInputStream inputStream;
    private InputStream innerInputStream;
    private OSSFileSystemStore ossFileSystemStore;

    @Before
    public void setUp() throws Exception {
        this.ossFileSystemStore = Mockito.mock(OSSFileSystemStore.class);
        this.innerInputStream = Mockito.mock(InputStream.class);
        Mockito.when(ossFileSystemStore.retrieve(anyString(), anyLong(), anyLong())).thenReturn(innerInputStream);
        Mockito.when(ossFileSystemStore.getOssFileLength(anyString())).thenReturn(FILE_LENGTH);
        Mockito.when(innerInputStream.read(any(), anyInt(), anyInt())).thenReturn(TEST_READ);
        Mockito.when(innerInputStream.skip(anyLong())).thenReturn(TEST_SKIP);
        this.inputStream = new FSOSSInputStream(ossFileSystemStore, OSS_KEY_PATH, BUFFER_SIZE);
    }

    @Test
    public void testAll() throws IOException {
        testSeek();
        testGetPos();
        testSeekToNewSource();
        testRead1();
        testRead2();
        testRead3();
        testSkip();
        testAvailable();
        testClose();
    }

    public void testSeek() throws IOException {
        inputStream.seek(TEST_SEEK_POS);
    }

    public void testGetPos() {
        Assert.assertEquals(TEST_SEEK_POS, inputStream.getPos());
    }

    public void testSeekToNewSource() {
        MockUtils.assertThrows(IOException.class, "seekToNewSource is not supported!",
            () -> inputStream.seekToNewSource(TEST_SEEK_POS));
    }

    public void testRead1() throws IOException {
        Assert.assertEquals(0, inputStream.read());
    }

    public void testRead2() throws IOException {
        Assert.assertEquals(1000, inputStream.read(new byte[1000]));
    }

    public void testRead3() throws IOException {
        Assert.assertEquals(TEST_READ - 1001, inputStream.read(new byte[TEST_READ], 0, TEST_READ));
    }

    public void testSkip() throws IOException {
        Assert.assertEquals(TEST_SKIP, inputStream.skip(TEST_SKIP));
    }

    public void testAvailable() throws IOException {
        Assert.assertEquals(FILE_LENGTH - TEST_SEEK_POS - TEST_READ - TEST_SKIP, inputStream.available());
    }

    public void testClose() throws IOException {
        inputStream.close();
    }
}
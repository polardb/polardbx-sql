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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.operator.spill;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.LongBlockBuilder;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.spill.LocalSpillMonitor;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import static com.google.common.util.concurrent.Futures.getUnchecked;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_OUT_OF_SPILL_FD;
import static com.alibaba.polardbx.common.utils.Assert.assertTrue;
import static com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory.getTemPath;
import static junit.framework.TestCase.assertEquals;

public class TestAsyncFileSingleStreamSpillerFactory {
    private final Closer closer = Closer.create();
    private ListeningExecutorService executor;
    private File spillPath1;
    private File spillPath2;
    private File spillTempPath1;
    private File spillTempPath2;

    @Before
    public void setUp() throws Exception {
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        closer.register(() -> executor.shutdownNow());
        spillPath1 =
            Paths.get("./tmp/" + this.getClass().getSimpleName() + "1").toAbsolutePath().toFile();
        spillTempPath1 = getTemPath(spillPath1);
        closer.register(() -> FileUtils.deleteDirectory(spillPath1));
        spillPath2 =
            Paths.get("./tmp/" + this.getClass().getSimpleName() + "2").toAbsolutePath().toFile();
        spillTempPath2 = getTemPath(spillPath2);
        closer.register(() -> FileUtils.deleteDirectory(spillPath2));
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().add(spillTempPath1.toPath());
        MppConfig.getInstance().getSpillPaths().add(spillTempPath2.toPath());
    }

    @After
    public void tearDown()
        throws Exception {
        closer.close();
    }

    @Test
    public void testDistributesSpillOverPaths() throws Exception {
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        AsyncFileSingleStreamSpillerFactory spillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), spillPaths, 4);

        assertEquals(FileUtils.listFiles(spillTempPath1, null, false).size(), 0);
        assertEquals(FileUtils.listFiles(spillTempPath2, null, false).size(), 0);

        Chunk page = buildChunk();
        List<SingleStreamSpiller> spillers = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            SingleStreamSpiller singleStreamSpiller =
                spillerFactory.create("testDistributesSpillOverPaths", ImmutableList.of(DataTypes.LongType),
                    new QuerySpillSpaceMonitor().newLocalSpillMonitor(), null);
            getUnchecked(singleStreamSpiller.spill(page));
            singleStreamSpiller.flush();
            spillers.add(singleStreamSpiller);
        }
        assertEquals(FileUtils.listFiles(spillTempPath1, null, false).size(), 5);
        assertEquals(FileUtils.listFiles(spillTempPath2, null, false).size(), 5);

        spillers.forEach(SingleStreamSpiller::close);
        assertEquals(FileUtils.listFiles(spillTempPath1, null, false).size(), 0);
        assertEquals(FileUtils.listFiles(spillTempPath2, null, false).size(), 0);
    }

    private Chunk buildChunk() {
        BlockBuilder col1 = new LongBlockBuilder(1);
        col1.writeLong(42);
        return new Chunk(col1.build());
    }

    @Test(expected = TddlRuntimeException.class)
    public void throwsIfNoDiskSpace() {
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        AsyncFileSingleStreamSpillerFactory spillerFactory =
            new AsyncFileSingleStreamSpillerFactory(new SyncFileCleaner(), spillPaths, 4, 0.0);

        spillerFactory.create("throwsIfNoDiskSpace", ImmutableList.of(DataTypes.LongType),
            new QuerySpillSpaceMonitor().newLocalSpillMonitor(), null);
    }

    @Test
    public void testCleanupOldSpillFiles() throws Exception {
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        spillTempPath1.mkdirs();
        spillTempPath2.mkdirs();

        java.nio.file.Files.createTempFile(spillTempPath1.toPath(), SingleStreamSpillerFactory.SPILL_FILE_PREFIX,
            SingleStreamSpillerFactory.SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillTempPath1.toPath(), SingleStreamSpillerFactory.SPILL_FILE_PREFIX,
            SingleStreamSpillerFactory.SPILL_FILE_SUFFIX);
        java.nio.file.Files
            .createTempFile(spillTempPath1.toPath(), SingleStreamSpillerFactory.SPILL_FILE_PREFIX, "blah");
        java.nio.file.Files.createTempFile(spillTempPath2.toPath(), SingleStreamSpillerFactory.SPILL_FILE_PREFIX,
            SingleStreamSpillerFactory.SPILL_FILE_SUFFIX);
        java.nio.file.Files
            .createTempFile(spillTempPath2.toPath(), "blah", SingleStreamSpillerFactory.SPILL_FILE_SUFFIX);
        java.nio.file.Files.createTempFile(spillTempPath2.toPath(), "blah", "blah");

        System.out.println(spillTempPath1.getAbsolutePath());
        System.out.println(spillTempPath2.getAbsolutePath());
        System.out.println(FileUtils.listFiles(spillTempPath1, null, false));
        assertEquals(FileUtils.listFiles(spillTempPath1, null, false).size(), 3);
        assertEquals(FileUtils.listFiles(spillTempPath2, null, false).size(), 3);

        AsyncFileSingleStreamSpillerFactory spillerFactory = new AsyncFileSingleStreamSpillerFactory(
            new SyncFileCleaner(),
            spillPaths,
            4);
        spillerFactory.cleanupOldSpillFiles();

        assertEquals(FileUtils.listFiles(spillTempPath1, null, false).size(), 1);
        assertEquals(FileUtils.listFiles(spillTempPath2, null, false).size(), 2);
    }

    @Test
    public void testMaxFdLimited() throws Exception {
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        spillTempPath1.mkdirs();
        spillTempPath2.mkdirs();

        AsyncFileSingleStreamSpillerFactory spillerFactory = new AsyncFileSingleStreamSpillerFactory(
            new SyncFileCleaner(),
            spillPaths,
            4,
            0.9,
            2);

        PagesSerdeFactory serdeFactory = new PagesSerdeFactory(false);
        FileHolder fd = spillerFactory.getNextFileHolder("default");

        LocalSpillMonitor localSpillMonitor = new QuerySpillSpaceMonitor().newLocalSpillMonitor();

        AsyncPageFileChannelWriter writer = spillerFactory
            .createAsyncPageFileChannelWriter(
                fd, serdeFactory.createPagesSerde(ImmutableList.of(DataTypes.LongType)), localSpillMonitor);
        AsyncPageFileChannelReader reader = spillerFactory
            .createAsyncPageFileChannelReader(fd, serdeFactory.createPagesSerde(ImmutableList.of(DataTypes.LongType)),
                localSpillMonitor);

        try {
            spillerFactory.createAsyncPageFileChannelWriter(spillerFactory.getNextFileHolder("default"),
                serdeFactory.createPagesSerde(ImmutableList.of(DataTypes.LongType)), localSpillMonitor);
            assertTrue(false);
        } catch (TddlRuntimeException expected) {
            assertEquals(expected.getErrorCodeType(), ERR_OUT_OF_SPILL_FD);
        }

        reader.close();

        spillerFactory.createAsyncPageFileChannelWriter(spillerFactory.getNextFileHolder("default"),
            serdeFactory.createPagesSerde(ImmutableList.of(DataTypes.LongType)), localSpillMonitor);

        writer.close(true);
    }
}

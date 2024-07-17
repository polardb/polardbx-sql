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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
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
import static org.junit.Assert.assertEquals;

public class TestFileSingleStreamSpillerFactory {
    private final Closer closer = Closer.create();
    private ListeningExecutorService executor;
    private File spillPath1;
    private File spillPath2;

    @Before
    public void setUp() throws Exception {
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        closer.register(() -> executor.shutdownNow());
        spillPath1 = Paths.get("./tmp/" + this.getClass().getSimpleName() + "1").toAbsolutePath().toFile();
        closer.register(() -> FileUtils.deleteDirectory(spillPath1));
        spillPath2 = Paths.get("./tmp/" + this.getClass().getSimpleName() + "2").toAbsolutePath().toFile();
        closer.register(() -> FileUtils.deleteDirectory(spillPath2));
    }

    @After
    public void tearDown()
        throws Exception {
        closer.close();
    }

    @Test
    public void testDistributesSpillOverPaths() throws Exception {
        List<DataType> types = ImmutableList.of(DataTypes.LongType);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
            executor, // executor won't be closed, because we don't call destroy() on the spiller factory
            spillPaths,
            1.0);

        assertEquals(FileUtils.listFiles(spillPath1, null, false).size(), 0);
        assertEquals(FileUtils.listFiles(spillPath2, null, false).size(), 0);

        Chunk page = buildChunk();
        List<SingleStreamSpiller> spillers = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            SingleStreamSpiller singleStreamSpiller = spillerFactory.create(
                "testDistributesSpillOverPaths", types, new QuerySpillSpaceMonitor("test").newLocalSpillMonitor(),
                null);
            getUnchecked(singleStreamSpiller.spill(page));
            singleStreamSpiller.flush();
            spillers.add(singleStreamSpiller);
        }
        assertEquals(FileUtils.listFiles(spillPath1, null, false).size(), 5);
        assertEquals(FileUtils.listFiles(spillPath2, null, false).size(), 5);

        spillers.forEach(SingleStreamSpiller::close);
        assertEquals(FileUtils.listFiles(spillPath1, null, false).size(), 0);
        assertEquals(FileUtils.listFiles(spillPath2, null, false).size(), 0);
    }

    private Chunk buildChunk() {
        BlockBuilder col1 = new LongBlockBuilder(1);
        col1.writeLong(42);
        return new Chunk(col1.build());
    }

    @Test(expected = TddlRuntimeException.class)
    public void throwsIfNoDiskSpace() {
        List<DataType> types = ImmutableList.of(DataTypes.LongType);
        List<Path> spillPaths = ImmutableList.of(spillPath1.toPath(), spillPath2.toPath());
        FileSingleStreamSpillerFactory spillerFactory = new FileSingleStreamSpillerFactory(
            executor, // executor won't be closed, because we don't call destroy() on the spiller factory
            spillPaths,
            0.0);

        spillerFactory.create("throwsIfNoDiskSpace", types, new QuerySpillSpaceMonitor("test").newLocalSpillMonitor(),
            null);
    }
}

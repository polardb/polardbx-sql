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
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.alibaba.polardbx.executor.operator.BaseExecTest.assertExecResultByRow;
import static com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleStreamSpillerFactory.getTemPath;
import static com.alibaba.polardbx.executor.operator.util.RowChunkBuilder.rowChunkBuilder;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static junit.framework.TestCase.assertEquals;

public class TestAsyncFileSingleStreamSpiller {
    private static final List<DataType> TYPES = ImmutableList.of(DataTypes.LongType, DataTypes.DoubleType);

    private final ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());
    private final File spillPath = Paths.get("./tmp/" + this.getClass().getSimpleName()).toAbsolutePath().toFile();

    @Before
    public void setUp() throws Exception {
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().add(spillPath.toPath());
    }

    @After
    public void tearDown()
        throws Exception {
        executor.shutdown();
        FileUtils.deleteDirectory(spillPath);
    }

    @Test
    public void testAsyncSpillerWriterWithTruncateMode()
        throws ExecutionException, InterruptedException {

        AsyncFileSingleStreamSpillerFactory factory = new AsyncFileSingleStreamSpillerFactory(
            new SyncFileCleaner(),
            ImmutableList.of(spillPath.toPath()), 4);

        AsyncFileSingleStreamSpiller spiller = (AsyncFileSingleStreamSpiller) factory.create(
            "testAsyncSpillerWriterWithTruncateMode", TYPES, new QuerySpillSpaceMonitor().newLocalSpillMonitor(), null);

        for (int i = 0; i < 100; i++) {
            spiller.spill(buildPage()).get();
        }

        spiller.flush();
        Iterator<Chunk> it = spiller.getSpilledChunks();
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        assertEquals(count, 100);

        count = 0;
        for (int i = 0; i < 10; i++) {
            spiller.spill(buildPage1()).get();
        }

        spiller.flush();
        spiller.reset();
        Iterator<Chunk> it1 = spiller.getSpilledChunks();
        while (it1.hasNext()) {
            it1.next();
            count++;
        }
        assertEquals(count, 10);
    }

    @Test
    public void testSpill() throws Exception {
        AsyncFileSingleStreamSpillerFactory factory = new AsyncFileSingleStreamSpillerFactory(
            new SyncFileCleaner(),
            ImmutableList.of(spillPath.toPath()), 4);
        AsyncFileSingleStreamSpiller spiller = (AsyncFileSingleStreamSpiller) factory.create(
            "testSpill", TYPES, new QuerySpillSpaceMonitor().newLocalSpillMonitor(), null);

        Chunk page = buildPage();

        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        assertEquals(1, FileUtils.listFiles(getTemPath(spillPath), null, false).size());

        spiller.flush();
        Iterator<Chunk> spilledPagesIterator = spiller.getSpilledChunks();
        // Async Context not using memory context per operator, so not check memoryContext
        //assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
        ImmutableList<Chunk> spilledPages = ImmutableList.copyOf(spilledPagesIterator);
        //assertEquals(memoryContext.getBytes(), 0);

        assertEquals(spilledPages.size(), 4);
        for (int i = 0; i < 4; ++i) {
            assertExecResultByRow(ImmutableList.of(page), ImmutableList.of(spilledPages.get(i)), true);
        }

        spiller.close();
        assertEquals(0, FileUtils.listFiles(getTemPath(spillPath), null, false).size());
    }

    @Test
    public void testGetAllSpilledPages()
        throws Exception {
        AsyncFileSingleStreamSpillerFactory factory = new AsyncFileSingleStreamSpillerFactory(
            new SyncFileCleaner(),
            ImmutableList.of(spillPath.toPath()), 4);
        AsyncFileSingleStreamSpiller spiller =
            (AsyncFileSingleStreamSpiller) factory.create(
                "testGetAllSpilledPages", TYPES, new QuerySpillSpaceMonitor().newLocalSpillMonitor(), null);

        Chunk page = buildPage();

        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        assertEquals(1, FileUtils.listFiles(getTemPath(spillPath), null, false).size());

        spiller.flush();
        ListenableFuture<List<Chunk>> pagesFuture = spiller.getAllSpilledChunks();
        List<Chunk> spilledPages = getFutureValue(pagesFuture);
        // Async Context not using memory context per operator, so not check memoryContext
        //assertEquals(memoryContext.getBytes(), FileSingleStreamSpiller.BUFFER_SIZE);
        //assertEquals(memoryContext.getBytes(), 0);

        assertEquals(spilledPages.size(), 4);
        for (int i = 0; i < 4; ++i) {
            assertExecResultByRow(ImmutableList.of(page), ImmutableList.of(spilledPages.get(i)), true);
        }

        spiller.close();
        assertEquals(0, FileUtils.listFiles(getTemPath(spillPath), null, false).size());
    }

    @Test
    public void testSpillReadClose()
        throws Exception {
        AsyncFileSingleStreamSpillerFactory factory = new AsyncFileSingleStreamSpillerFactory(
            new SyncFileCleaner(),
            ImmutableList.of(spillPath.toPath()), 1);
        AsyncFileSingleStreamSpiller spiller =
            (AsyncFileSingleStreamSpiller) factory.create(
                "testSpillReadClose", TYPES, new QuerySpillSpaceMonitor().newLocalSpillMonitor(), null);

        Chunk page = buildPage();

        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        assertEquals(1, FileUtils.listFiles(getTemPath(spillPath), null, false).size());

        spiller.flush();
        factory.getReadThread(0).getQueue().setBlocked(true);
        spiller.close();
        assertEquals(0, FileUtils.listFiles(getTemPath(spillPath), null, false).size());
    }

    private Chunk buildPage() {
        return rowChunkBuilder(DataTypes.LongType, DataTypes.DoubleType)
            .row(42L, 43.0)
            .build();
    }

    private Chunk buildPage1() {
        return rowChunkBuilder(DataTypes.LongType, DataTypes.DoubleType)
            .row(42L, 43.0)
            .build();
    }
}

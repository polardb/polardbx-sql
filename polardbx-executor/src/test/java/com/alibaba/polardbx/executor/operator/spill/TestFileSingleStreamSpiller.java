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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerdeFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.alibaba.polardbx.executor.operator.BaseExecTest.assertExecResultByRow;
import static com.alibaba.polardbx.executor.operator.util.RowChunkBuilder.rowChunkBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static junit.framework.TestCase.assertEquals;

public class TestFileSingleStreamSpiller {

    private final ListeningExecutorService executor = listeningDecorator(newCachedThreadPool());
    private final File spillPath = Paths.get("./tmp/" + this.getClass().getSimpleName()).toAbsolutePath().toFile();

    @Before
    public void setUp() throws Exception {
        spillPath.mkdirs();
        MppConfig.getInstance().getSpillPaths().clear();
        MppConfig.getInstance().getSpillPaths().add(spillPath.toPath());
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdown();
        FileUtils.deleteDirectory(spillPath);
    }

    @Test
    public void testSpill()
        throws Exception {
        PagesSerdeFactory serdeFactory = new PagesSerdeFactory(false);
        PagesSerde serde = serdeFactory.createPagesSerde(ImmutableList.of(
            DataTypes.LongType, DataTypes.DoubleType));
        FileSingleStreamSpiller spiller =
            new FileSingleStreamSpiller(serde, executor, new SyncFileCleaner(), spillPath.toPath(),
                new QuerySpillSpaceMonitor().newLocalSpillMonitor());

        Chunk page = buildPage();

        spiller.spill(page).get();
        spiller.spill(Iterators.forArray(page, page, page)).get();
        assertEquals(1, FileUtils.listFiles(spillPath, null, false).size());
        spiller.flush();
        ImmutableList<Chunk> spilledPages = ImmutableList.copyOf(spiller.getSpilledChunks());

        assertEquals(4, spilledPages.size());
        for (int i = 0; i < 4; ++i) {
            assertExecResultByRow(ImmutableList.of(page), ImmutableList.of(spilledPages.get(i)), true);
        }

        spiller.close();
        assertEquals(0, FileUtils.listFiles(spillPath, null, false).size());
    }

    private Chunk buildPage() {
        return rowChunkBuilder(DataTypes.LongType, DataTypes.DoubleType)
            .row(42L, 43.0)
            .build();
    }
}

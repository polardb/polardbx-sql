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

import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.execution.buffer.PagesSerde;
import com.alibaba.polardbx.optimizer.spill.LocalSpillMonitor;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AsyncFileSingleStreamSpiller
    implements SingleStreamSpiller {
    private static final Logger log = LoggerFactory.getLogger(AsyncFileSingleStreamSpiller.class);

    private final FileHolder id;
    private final PagesSerde serde;
    private final AsyncFileSingleStreamSpillerFactory factory;

    private AsyncPageFileChannelReader reader;
    private AsyncPageFileChannelWriter writer;
    private LocalSpillMonitor spillMonitor;

    public AsyncFileSingleStreamSpiller(
        AsyncFileSingleStreamSpillerFactory factory,
        FileHolder id,
        PagesSerde serde,
        LocalSpillMonitor spillMonitor) {
        this.id = requireNonNull(id, "fileId is null");
        this.serde = requireNonNull(serde, "serde is null");
        this.factory = requireNonNull(factory, "factory is null");
        try {
            id.getFilePath().toFile().createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.spillMonitor = spillMonitor;
    }

    private void ensureReader() {
        // GenericSpiller only use spill(Iterator<Page> page) method without invoke flush
        if (writer != null) {
            flush();
        }
        checkState(writer == null, "writer is not null");
        if (reader == null) {
            try {
                reader = factory.createAsyncPageFileChannelReader(id, serde, spillMonitor);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void ensureWriter() {
        if (writer == null) {
            try {
                writer = factory.createAsyncPageFileChannelWriter(id, serde, spillMonitor);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Chunk> pages) {
        try {
            ensureWriter();
            return writer.writePages(pages);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ListenableFuture<?> spill(Chunk page) {
        try {
            ensureWriter();
            return writer.writePage(page);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Chunk> getSpilledChunks() {
        ensureReader();
        try {
            return reader.readPagesIterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ListenableFuture<List<Chunk>> getAllSpilledChunks() {
        ensureReader();
        try {
            return reader.readPagesList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void flush() {
        closeWriter(false);
    }

    @Override
    public void reset() {
        closeReader();
        reader = null;
    }

    @Override
    public void close() {
        try (Closer closer = Closer.create()) {
            log.info(
                String.format(
                    "AsyncFileSingleStreamSpiller file:%s total spilled bytes:%s", id.getFilePath(),
                    spillMonitor.totalSpilledBytes()));
            closer.register(id);
            closer.register(spillMonitor);
            closer.register(this::closeReader);
            closer.register(() -> closeWriter(true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeReader() {
        if (reader != null) {
            try {
                reader.close();
                reader = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void closeWriter(boolean errorClose) {
        if (writer != null) {
            try {
                writer.close(errorClose);
                writer = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String toString() {
        return "AsyncFileSingleStreamSpiller{" +
            "id=" + id.getFilePath() +
            '}';
    }
}

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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.spill.LocalSpillMonitor;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.sql.OutFileParams;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class AsyncFileSingleBufferSpiller implements SingleStreamSpiller {
    private static final Logger log = LoggerFactory.getLogger(AsyncFileSingleBufferSpiller.class);

    private final FileHolder id;
    private final List<ColumnMeta> columns;
    private final OutFileParams outFileParams;
    private final AsyncFileSingleStreamSpillerFactory factory;

    private AsyncFileBufferWriter writer;
    private LocalSpillMonitor spillMonitor;

    public AsyncFileSingleBufferSpiller(
        AsyncFileSingleStreamSpillerFactory factory,
        FileHolder id,
        List<ColumnMeta> columns,
        OutFileParams outFileParams,
        LocalSpillMonitor spillMonitor) {
        this.id = requireNonNull(id, "fileId is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.outFileParams = requireNonNull(outFileParams, "outFileParams is null");
        this.factory = requireNonNull(factory, "factory is null");
        if (id.getFilePath().toFile().exists()) {
            if (outFileParams.getStatistics()) {
                id.getFilePath().toFile().delete();
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_FILE_ALREADY_EXIST, "File is already exist!");
            }
        }
        try {
            id.getFilePath().toFile().createNewFile();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_FILE_CANNOT_BE_CREATE,
                "OutFile cannot be created at the server!");
        }
        this.spillMonitor = spillMonitor;
    }

    private void ensureWriter() {
        if (writer == null) {
            try {
                writer = factory.createAsyncPageFileChannelWriter(id, columns, outFileParams, spillMonitor);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ListenableFuture<?> spill(Iterator<Chunk> pages) {
        throw new UnsupportedOperationException();
    }

    public ListenableFuture<?> spillRows(Iterator<Row> rows, long size) {
        try {
            ensureWriter();
            return writer.writeRows(rows, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Chunk> getSpilledChunks() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<List<Chunk>> getAllSpilledChunks() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() {
        writer.flush();
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
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
            closer.register(() -> closeWriter(true));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeWriter(boolean errorClose) {
        if (writer != null) {
            try {
                spillMonitor.close();
                writer.close(errorClose);
                writer = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public String toString() {
        return "AsyncFileSingleBufferSpiller{" +
            "id=" + id.getFilePath() +
            '}';
    }
}

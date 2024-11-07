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

package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.hadoop.fs.FSDataInputStream;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple implementation of csv file reader.
 * It will load all .csv file bytes into memory, and parse bytes into blocks line by line.
 */
public class SimpleCSVFileReader implements CSVFileReader {
    private int fieldNum;
    private FSDataInputStream inputStream;
    private List<ColumnProvider> columnProviders;
    private List<ColumnMeta> columnMetas;
    private ByteCSVReader rowReader;
    private ExecutionContext context;
    private int chunkLimit;
    private int offset;
    private int length;

    @Override
    public void open(ExecutionContext context,
                     List<ColumnMeta> columnMetas,
                     int chunkLimit,
                     Engine engine,
                     String csvFileName,
                     int offset,
                     int length) throws IOException {
        this.chunkLimit = chunkLimit;
        this.context = context;
        this.fieldNum = columnMetas.size();

        this.inputStream = FileSystemUtils.openStreamFileWithBuffer(csvFileName, engine, true);
        if (offset > 0) {
            inputStream.seek(offset);
        }
        this.length = length == EOF ? Integer.MAX_VALUE : length;

        this.columnProviders = columnMetas.stream()
            .map(ColumnProviders::getProvider).collect(Collectors.toList());
        this.columnMetas = columnMetas;
        this.rowReader = new ByteCSVReader(csvFileName, inputStream, this.length);
        this.offset = offset;
    }

    @Override
    public Chunk next() {
        return nextUntilPosition(Long.MAX_VALUE);
    }

    @Override
    public Chunk nextUntilPosition(long pos) {
        long positionBound = Math.min(pos, offset + length);
        List<BlockBuilder> blockBuilders = this.columnMetas
            .stream()
            .map(ColumnMeta::getDataType)
            .map(t -> BlockBuilders.create(t, context))
            .collect(Collectors.toList());

        int totalRow = 0;
        try {
            while (offset + rowReader.position() < positionBound && rowReader.isReadable()) {
                CSVRow row = rowReader.nextRow();

                // for each row, parse each column and append onto block-builder
                for (int columnId = 0; columnId < fieldNum; columnId++) {
                    ColumnProvider columnProvider = columnProviders.get(columnId);
                    BlockBuilder blockBuilder = blockBuilders.get(columnId);
                    DataType dataType = columnMetas.get(columnId).getDataType();

                    columnProvider.parseRow(
                        blockBuilder, row, columnId, dataType
                    );
                }

                // reach chunk limit
                if (++totalRow >= chunkLimit) {
                    return buildChunk(blockBuilders, totalRow);
                }
            }
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }

        // flush the remaining rows
        return totalRow == 0 ? null : buildChunk(blockBuilders, totalRow);
    }

    @NotNull
    protected Chunk buildChunk(List<BlockBuilder> blockBuilders, int totalRow) {
        return new Chunk(totalRow, blockBuilders.stream()
            .map(BlockBuilder::build).toArray(Block[]::new));
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    public long position() throws IOException {
        return offset + rowReader.position();
    }
}

package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.columnar.SimpleCSVFileReader.buildChunk;

public class AutoFlashbackCSVFileReader implements CSVFileReader {
    private int fieldNum;
    private InputStream inputStream;
    private List<ColumnProvider> columnProviders;
    private List<ColumnMeta> columnMetas;
    private ByteCSVReader rowReader;
    private ExecutionContext context;
    private int chunkLimit;
    private long tso;
    private boolean isFinished = false;

    @Override
    public void open(ExecutionContext context, List<ColumnMeta> columnMetas, int chunkLimit, Engine engine,
                     String csvFileName, int offset, int length) throws IOException {
        throw new NotSupportException("Designate offset and length is not support for auto flashback reader");
    }

    public void open(ExecutionContext context, List<ColumnMeta> columnMetas, int chunkLimit, Engine engine,
                     String csvFileName, long tso, int maxLength)
        throws IOException {
        this.chunkLimit = chunkLimit;
        this.context = context;
        this.fieldNum = columnMetas.size();

        byte[] buffer = new byte[maxLength];
        FileSystemUtils.readFile(csvFileName, 0, maxLength, buffer, engine, true);

        this.inputStream = new ByteArrayInputStream(buffer);
        this.columnProviders = columnMetas.stream()
            .map(ColumnProviders::getProvider).collect(Collectors.toList());
        this.columnMetas = columnMetas;

        byte[] reusableNulls = new byte[fieldNum];
        int[] reusableOffsets = new int[fieldNum];

        this.rowReader = new ByteCSVReader(csvFileName, inputStream, -1, reusableNulls, reusableOffsets);
        this.tso = tso;
    }

    @Override
    public Chunk next() {
        List<BlockBuilder> blockBuilders = this.columnMetas
            .stream()
            .map(ColumnMeta::getDataType)
            .map(t -> BlockBuilders.create(t, context, chunkLimit))
            .collect(Collectors.toList());
        int totalRow = 0;
        try {
            while (rowReader.isReadable() && !isFinished) {
                CSVRow row = rowReader.nextRow();

                // for TSO column, deal it directly
                ByteBuffer bytes = row.getBytes(0);
                long longVal = ColumnProvider.longFromByte(bytes);
                if (longVal >= tso) {
                    // end read with tso
                    isFinished = true;
                    break;
                }
                blockBuilders.get(0).writeLong(longVal);

                // for each row, parse each column and append onto block-builder
                for (int columnId = 1; columnId < fieldNum; columnId++) {
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

    @Override
    public Chunk nextUntilPosition(long pos) {
        return next();
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }
}

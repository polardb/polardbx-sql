package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.archive.reader.TypeComparison;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockConverter;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.Converters;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.TimestampUtils;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.time.ZoneId;
import java.util.BitSet;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

public class CsvScanWork extends AbstractScanWork {
    private static final Logger logger = LoggerFactory.getLogger("COLUMNAR_TRANS");

    private final ColumnarManager columnarManager;

    private final ExecutionContext executionContext;

    private final long tso;

    private final Path csvFile;

    private final boolean useSelection;

    private final boolean enableCompatible;

    private final List<Integer> refList;

    private final TimeZone targetTimeZone;

    public CsvScanWork(ColumnarManager columnarManager, long tso, Path csvFile,
                       List<Integer> inputRefsForFilter,
                       List<Integer> inputRefsForProject,
                       ExecutionContext executionContext,
                       String workId, RuntimeMetrics metrics, boolean enableMetrics,
                       LazyEvaluator<Chunk, BitSet> lazyEvaluator, RoaringBitmap deletion,
                       int partNum, int nodePartCount, boolean useSelection, boolean enableCompatible,
                       OSSColumnTransformer ossColumnTransformer) {
        super(workId, metrics, enableMetrics, lazyEvaluator, null, deletion, null, inputRefsForFilter,
            inputRefsForProject, partNum, nodePartCount, ossColumnTransformer);
        this.columnarManager = columnarManager;
        this.tso = tso;
        this.csvFile = csvFile;
        this.useSelection = useSelection;
        this.enableCompatible = enableCompatible;
        this.executionContext = executionContext;
        this.targetTimeZone = TimestampUtils.getTimeZone(executionContext);
        refList = refSet.stream().sorted().collect(Collectors.toList());
    }

    protected void handleNextWork() throws Throwable {
        List<Chunk> chunkList;
        if (executionContext.isEnableOrcRawTypeBlock()) {
            // Special csv scan work for raw orc type.
            // Only Long/Double/ByteArray blocks are created.
            // Normal query should not get there.
            chunkList = columnarManager.rawCsvData(tso, csvFile.getName(), executionContext);
        } else {
            chunkList = columnarManager.csvData(tso, csvFile.getName());
        }
        int filterColumns = inputRefsForFilter.size();

        boolean skipEvaluation = filterColumns == 0;
        int totalPositionCnt = 0;
        for (Chunk chunk : chunkList) {
            if (isCanceled) {
                break;
            }

            chunk = projectCsvChunk(chunk);
            int positionCnt = chunk.getPositionCount();
            int[] selection = null;

            if (!skipEvaluation) {
                long start = System.nanoTime();

                selection = selectionOf(lazyEvaluator.eval(chunk, totalPositionCnt, positionCnt, deletionBitmap));

                if (enableMetrics) {
                    evaluationTimer.inc(System.nanoTime() - start);
                }
            } else {
                selection = selectionOf(new int[] {totalPositionCnt, positionCnt}, deletionBitmap);
            }

            // NULL selection means full selection here
            if (selection == null) {
                ioStatus.addResult(chunk);
            } else if (selection.length > 0) {
                // rebuild chunk according to project refs.
                Chunk projectChunk = rebuildProject(chunk, selection, selection.length);
                ioStatus.addResult(projectChunk);
            }
            totalPositionCnt += positionCnt;
        }

        logger.debug(
            String.format("Csv scan work finished: chunk count: %d, row count: %d, row/chunk: %f", chunkList.size(),
                totalPositionCnt, (double) totalPositionCnt / chunkList.size()));

        ioStatus.finish();
    }

    private Chunk projectCsvChunk(Chunk chunk) {
        Block[] blocks = new Block[refList.size()];
        int blockIndex = 0;

        for (int i = 0; i < refList.size(); i++) {
            final Integer columnId = columnTransformer.getLocInOrc(chunkRefMap[refList.get(i)]);

            ColumnMeta sourceColumnMeta = columnTransformer.getSourceColumnMeta(i);
            ColumnMeta targetColumnMeta = columnTransformer.getTargetColumnMeta(i);
            TypeComparison comparison = columnTransformer.getCompareResult(i);
            Block block;

            switch (comparison) {
            case MISSING_EQUAL:
                block = OSSColumnTransformer.fillDefaultValue(
                    targetColumnMeta.getDataType(),
                    columnTransformer.getInitColumnMeta(i),
                    columnTransformer.getTimeStamp(i),
                    chunk.getPositionCount(),
                    executionContext
                );
                break;
            case MISSING_NO_EQUAL:
                block = OSSColumnTransformer.fillDefaultValueAndTransform(
                    targetColumnMeta,
                    columnTransformer.getInitColumnMeta(i),
                    chunk.getPositionCount(),
                    executionContext
                );
                break;
            default:
                BlockConverter converter = Converters.createBlockConverter(
                    sourceColumnMeta.getDataType(),
                    targetColumnMeta.getDataType(),
                    executionContext
                );
                block = converter.apply(chunk.getBlock(columnId - 1));
                break;
            }

            if (block instanceof TimestampBlock) {
                block = TimestampBlock.from((TimestampBlock) block, targetTimeZone);
            }
            blocks[blockIndex++] = block;
        }

        Chunk result = new Chunk(blocks);
        result.setPartIndex(partNum);
        result.setPartCount(nodePartCount);
        return result;
    }

    protected Chunk rebuildProject(Chunk chunk, int[] selection, int selSize) {
        Block[] blocks = new Block[inputRefsForProject.size()];
        int blockIndex = 0;

        // if all positions are selected, we should not use selection array.
        boolean fullySelected = chunk.getPositionCount() == selSize;

        for (int projectRef : inputRefsForProject) {
            // mapping blocks for projection.
            int chunkIndex = chunkRefMap[projectRef];
            Block block = chunk.getBlock(chunkIndex);

            if (!fullySelected) {
                blocks[blockIndex++] = BlockUtils.fillSelection(block, selection, selSize,
                    useSelection, enableCompatible, targetTimeZone);
            } else {
                blocks[blockIndex++] = block;
            }
        }

        Chunk result = new Chunk(blocks);
        result.setPartIndex(partNum);
        result.setPartCount(nodePartCount);
        return result;
    }

    @Override
    public void invoke(ExecutorService executor) {
        executor.submit(() -> {
            try {
                handleNextWork();
            } catch (Throwable e) {
                ioStatus.addException(e);
                LOGGER.error("fail to execute csv scan work: ", e);
            }
        });
    }

    @Override
    public IOStatus<Chunk> getIOStatus() {
        return ioStatus;
    }

    @Override
    public String getWorkId() {
        return workId;
    }

    @Override
    public RuntimeMetrics getMetrics() {
        return metrics;
    }

    @Override
    public void close() throws IOException {
        if (ioStatus != null) {
            ioStatus.close();
        }
    }
}

package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.MultiVersionCsvData;
import com.alibaba.polardbx.executor.operator.scan.LazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.metrics.RuntimeMetrics;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.hadoop.fs.Path;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

/**
 * This csv scan work scan csv data of specified start and end positions,
 * and with specified delete bitmap.
 *
 * @author wuzhe
 */
public class SpecifiedCsvScanWork extends CsvScanWork {
    private static final Logger logger = LoggerFactory.getLogger("COLUMNAR_TRANS");

    final private int start;
    final private int end;

    final private long tsoV0;

    final private long tsoV1;

    public SpecifiedCsvScanWork(ColumnarManager columnarManager, long tso,
                                Path csvFile, List<Integer> inputRefsForFilter,
                                List<Integer> inputRefsForProject,
                                ExecutionContext executionContext, String workId,
                                RuntimeMetrics metrics,
                                boolean enableMetrics,
                                LazyEvaluator<Chunk, BitSet> lazyEvaluator,
                                RoaringBitmap deletion, int partNum, int nodePartCount,
                                boolean useSelection, boolean enableCompatible,
                                OSSColumnTransformer ossColumnTransformer,
                                int start, int end, long tsoV0, long tsoV1) {
        super(columnarManager, tso, null, csvFile, inputRefsForFilter, inputRefsForProject, executionContext, workId,
            metrics,
            enableMetrics, lazyEvaluator, deletion, partNum, nodePartCount, useSelection, enableCompatible,
            ossColumnTransformer);
        this.start = start;
        this.end = end;
        this.tsoV0 = tsoV0;
        this.tsoV1 = tsoV1;
    }

    @Override
    protected void handleNextWork() throws Throwable {
        Iterator<Chunk> chunkIterator;
        if (executionContext.isEnableOrcRawTypeBlock()) {
            // Special csv scan work for raw orc type.
            // Only Long/Double/ByteArray blocks are created.
            // Normal query should not get there.
            chunkIterator = MultiVersionCsvData.loadRawOrcTypeUntilTso(csvFile.getName(), executionContext, start, end);
        } else {
            chunkIterator = MultiVersionCsvData.loadSpecifiedCsvFile(csvFile.getName(), executionContext, start, end);
        }

        int filterColumns = inputRefsForFilter.size();

        boolean skipEvaluation = filterColumns == 0;
        int totalPositionCnt = 0;
        int totalChunkCnt = 0;
        while (chunkIterator.hasNext()) {
            Chunk chunk = chunkIterator.next();
            if (isCanceled) {
                break;
            }

            Block tsoBlock = chunk.getBlock(0);
            int starPos = (int) chunk.getBlock(1).getLong(0);

            chunk = projectCsvChunk(chunk);
            int positionCnt = chunk.getPositionCount();
            int[] selection = null;

            if (!skipEvaluation) {
                long start = System.nanoTime();

                selection = selectionOf(lazyEvaluator.eval(chunk, starPos, positionCnt, deletionBitmap));

                if (enableMetrics) {
                    evaluationTimer.inc(System.nanoTime() - start);
                }
            } else {
                selection = selectionOf(new int[] {starPos, positionCnt}, deletionBitmap);
            }

            if (tsoV0 > 0 && tsoV1 > 0) {
                // Filter by specified tso versions.
                selection = filterTso(selection, tsoBlock);
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
            totalChunkCnt++;
        }

        logger.debug(
            String.format("Specified csv scan work finished: chunk count: %d, row count: %d, row/chunk: %f",
                totalChunkCnt,
                totalPositionCnt, totalChunkCnt == 0 ? -1 : (double) totalPositionCnt / totalChunkCnt));

        ioStatus.finish();
    }

    private int[] filterTso(int[] selection, Block tso) {
        int[] result = null;
        List<Integer> resultList = new ArrayList<>();
        if (null == selection) {
            for (int i = 0; i < tso.getPositionCount(); i++) {
                if (!tso.isNull(i) && tso.getLong(i) > tsoV0 && tso.getLong(i) <= tsoV1) {
                    resultList.add(i);
                }
            }
            if (resultList.size() == tso.getPositionCount()) {
                // All selected, return null.
                return result;
            }
        } else {
            for (int pos : selection) {
                if (!tso.isNull(pos) && tso.getLong(pos) > tsoV0 && tso.getLong(pos) <= tsoV1) {
                    resultList.add(pos);
                }
            }
        }
        result = new int[resultList.size()];
        for (int i = 0; i < resultList.size(); i++) {
            result[i] = resultList.get(i);
        }
        return result;
    }
}

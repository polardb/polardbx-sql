package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.mpp.planner.FragmentRFManager;
import com.alibaba.polardbx.executor.operator.scan.ColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.IOStatus;
import com.alibaba.polardbx.executor.operator.scan.ScanState;
import com.alibaba.polardbx.executor.operator.scan.ScanWork;
import com.alibaba.polardbx.executor.operator.scan.ScanWorkTest;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.statis.OperatorStatistics;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.orc.TypeDescription;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils.TYPE_FACTORY;
import static org.mockito.Mockito.mock;

public class SpecifiedOrcColumnarSplitTest extends ScanWorkTest {
    public static TypeDescription SCHEMA = TypeDescription.createStruct();
    protected TypeDescription backupSchema = null;
    protected List<ColumnMeta> backupColumnMeta = null;
    protected List<Integer> backLocInOrc = null;
    private long tsoV0 = 100L;
    private long tsoV1 = 200L;

    static {
        SCHEMA.addField("pk", TypeDescription.createLong());
        SCHEMA.addField("i0", TypeDescription.createLong());
        SCHEMA.addField("order_id", TypeDescription.createString());
        SCHEMA.addField("v0", TypeDescription.createString());
    }

    @Test
    @Override
    public void test1() {
        setTso();
        ColumnarSplit split = getSplit();
        Assert.assertTrue(split instanceof SpecifiedOrcColumnarSplit);
    }

    @After
    public void after() {
        context.getParamManager().getProps().put("CCI_INCREMENTAL_CHECK", "false");
        if (backupColumnMeta != null) {
            COLUMN_METAS.clear();
            COLUMN_METAS.addAll(backupColumnMeta);
        }
        if (backLocInOrc != null) {
            LOC_IN_ORC.clear();
            LOC_IN_ORC.addAll(backLocInOrc);
        }
        if (backupSchema != null) {
            SCHEMA = backupSchema;
        }
    }

    @Test
    @Override
    public void test2() throws ExecutionException, InterruptedException {
        setTso();
        // 100-200: nothing can be seen
        tsoV0 = 100L;
        tsoV1 = 200L;
        context.getParamManager().getProps().put("CCI_INCREMENTAL_CHECK", "true");
        shouldSeenRows(0);
    }

    @Test
    @Override
    public void test3() throws ExecutionException, InterruptedException {
        setTso();
        // 100000-100001: should see only one row
        tsoV0 = 1000000L;
        tsoV1 = 1000001L;
        context.getParamManager().getProps().put("CCI_INCREMENTAL_CHECK", "true");
        shouldSeenRows(1);
    }

    @Test
    @Override
    public void test4() throws ExecutionException, InterruptedException {
        setTso();
        // 900000-900001: should see nothing
        tsoV0 = 9000000L;
        tsoV1 = 9000001L;
        context.getParamManager().getProps().put("CCI_INCREMENTAL_CHECK", "true");
        shouldSeenRows(0);
    }

    @Test
    @Override
    public void test5() throws ExecutionException, InterruptedException {
        setTso();
        // 100000-100001: should see only one row
        tsoV0 = 1000000L;
        tsoV1 = 1000001L;
        context.getParamManager().getProps().put("ENABLE_ORC_RAW_TYPE_BLOCK", "true");
        shouldSeenRows(1);
    }

    private void shouldSeenRows(long expected) throws InterruptedException, ExecutionException {
        ColumnarSplit split = getSplit();
        ScanWork<ColumnarSplit, Chunk> scanWork;
        long actual = 0;
        while ((scanWork = split.nextWork()) != null) {
            System.out.println(scanWork.getWorkId());

            MorselColumnarSplit.ScanRange scanRange =
                ((AbstractScanWork) scanWork).getScanRange();
            System.out.println("scan range: " + scanRange);

            // get status
            IOStatus<Chunk> ioStatus = scanWork.getIOStatus();
            scanWork.invoke(SCAN_WORK_EXECUTOR);

            // Get chunks according to state.
            boolean isCompleted = false;
            while (!isCompleted) {
                ScanState state = ioStatus.state();
                Chunk result;
                switch (state) {
                case READY:
                case BLOCKED: {
                    result = ioStatus.popResult();
                    if (result == null) {
                        ListenableFuture<?> listenableFuture = ioStatus.isBlocked();
                        listenableFuture.get();
                        result = ioStatus.popResult();
                        actual += null == result ? 0 : result.getPositionCount();
                    }
                    break;
                }
                case FINISHED:
                    while ((result = ioStatus.popResult()) != null) {
                        actual += result.getPositionCount();
                    }
                    isCompleted = true;
                    break;
                case FAILED:
                    isCompleted = true;
                    ioStatus.throwIfFailed();
                    break;
                case CLOSED:
                    isCompleted = true;
                    break;
                }

            }
            Preconditions.checkArgument(((AbstractScanWork) scanWork).checkIfAllReadersClosed());
        }
        Assert.assertTrue(expected == actual);
    }

    private void setTso() {
        backupSchema = SCHEMA;
        SCHEMA = TypeDescription.createStruct();
        SCHEMA.addField("tso", TypeDescription.createLong());
        SCHEMA.addField("pos", TypeDescription.createLong());
        SCHEMA.addField("order_id", TypeDescription.createString());
        SCHEMA.addField("v0", TypeDescription.createString());

        backupColumnMeta = new ArrayList<>(COLUMN_METAS);
        COLUMN_METAS.clear();
        COLUMN_METAS.add(new ColumnMeta("t1", "order_id", "order_id", new Field(new VarcharType())));
        COLUMN_METAS.add(new ColumnMeta("t1", "v0", "v0", new Field(new VarcharType())));

        backLocInOrc = new ArrayList<>(LOC_IN_ORC);
        LOC_IN_ORC.clear();
        LOC_IN_ORC.add(3);
        LOC_IN_ORC.add(4);
    }

    private ColumnarSplit getSplit() {
        Engine engine = Engine.LOCAL_DISK;
        String schema = "test_schema";
        Set<Integer> refSet = new TreeSet<>();
        List<Integer> inputRefsForProject = ImmutableList.of(0);
        refSet.addAll(inputRefsForProject);
        List<ColumnMeta> columnMetas = refSet.stream().map(COLUMN_METAS::get).collect(Collectors.toList());
        List<Integer> locInOrc = refSet.stream().map(LOC_IN_ORC::get).collect(Collectors.toList());
        ColumnMeta tsoColumnMeta = new ColumnMeta("t1", "tso", null,
            new Field(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT)));
        columnMetas.add(tsoColumnMeta);
        locInOrc.add(1);
        OSSColumnTransformer ossColumnTransformer =
            new OSSColumnTransformer(columnMetas, columnMetas, null, null, locInOrc);
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        NonBlockedScanPreProcessor preProcessor = new NonBlockedScanPreProcessor(
            preheatFileMeta, matrix, deletionBitmap
        );
        preProcessor.addFile(FILE_PATH);
        FragmentRFManager fragmentRFManager = mock(FragmentRFManager.class);
        ColumnarManager columnarManager = mock(ColumnarManager.class);
        OperatorStatistics operatorStatistics = mock(OperatorStatistics.class);

        ColumnarSplit.ColumnarSplitBuilder builder = SpecifiedOrcColumnarSplit.newBuilder();
        builder.executionContext(context);
        builder.ioExecutor(IO_EXECUTOR);
        builder.fileSystem(FILESYSTEM, engine);
        builder.configuration(CONFIGURATION);
        builder.sequenceId(1);
        builder.file(FILE_PATH, 1);
        builder.tableMeta(schema, "test_table");
        builder.columnTransformer(ossColumnTransformer);
        builder.inputRefs(inputRefsForFilter, inputRefsForProject);
        builder.cacheManager(blockCacheManager);
        builder.chunkLimit(DEFAULT_CHUNK_LIMIT);
        builder.morselUnit(1024);
        builder.pushDown(evaluator);
        builder.prepare(preProcessor);
        builder.columnarManager(columnarManager);
        builder.isColumnarMode(true);
        builder.tso(100L);
        builder.position(100L);
        builder.partNum(8);
        builder.nodePartCount(8);
        builder.memoryAllocator(memoryAllocatorCtx);
        builder.fragmentRFManager(fragmentRFManager);
        builder.operatorStatistic(operatorStatistics);
        builder.tsoV0(tsoV0);
        builder.tsoV1(tsoV1);
        return builder.build();
    }
}

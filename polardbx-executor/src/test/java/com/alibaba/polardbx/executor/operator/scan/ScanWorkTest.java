package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.archive.reader.OSSColumnTransformer;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.impl.AbstractScanWork;
import com.alibaba.polardbx.executor.operator.scan.impl.DefaultLazyEvaluator;
import com.alibaba.polardbx.executor.operator.scan.impl.MorselColumnarSplit;
import com.alibaba.polardbx.executor.operator.scan.impl.NonBlockedScanPreProcessor;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.io.Closeable;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.BitSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;

public class ScanWorkTest extends ScanTestBase {
    protected final static ExecutorService SCAN_WORK_EXECUTOR = Executors.newFixedThreadPool(4);

    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    public static final int MORSEL_UNIT = 5;
    public static final double RATIO = .3D;

    // need trace_id or other session-level parameters.
    protected ExecutionContext context;

    // partial cache
    protected BlockCacheManager<Block> blockCacheManager;

    // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
    private RexNode predicate;
    protected List<Integer> inputRefsForFilter;
    protected LazyEvaluator<Chunk, BitSet> evaluator;
    protected RoaringBitmap deletionBitmap;

    @Before
    public void prepare() throws IOException {
        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);

        // Cached ranges
        final int cachedStripeId = 0;
        final int[] columnIds = new int[] {1, 2};
        final int[] cachedGroupIds = new int[] {0, 2};
        blockCacheManager = prepareCache(
            cachedStripeId, columnIds, cachedGroupIds
        );

        // ($0 + 10000L) >= 0L
        predicate = buildCondition(0,
            TddlOperatorTable.PLUS, 10000L,
            TddlOperatorTable.GREATER_THAN_OR_EQUAL, 0L
        );

        // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
        evaluator = DefaultLazyEvaluator.builder()
            .setContext(context)
            .setRexNode(predicate)
            .setRatio(RATIO)
            .setInputTypes(ImmutableList.of(DataTypes.LongType)) // input type: bigint.
            .build();

        inputRefsForFilter = ImmutableList.of(0);

        deletionBitmap = buildDeletionBitmap((int) (1_000_000L / 2));
    }

    @Test
    public void test1() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2)
        );
    }

    @Test
    public void test2() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));

        doTest(
            matrix,
            ImmutableList.of(0, 1, 2, 3)
        );
    }

    @Test
    public void test3() throws Throwable {
        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(4, fromRowGroupIds(4, new int[] {2, 3}));
        doTest(
            matrix,
            ImmutableList.of(0, 2, 3)
        );
    }

    @Test
    public void test4() throws Throwable {
        // $1 as filter column
        inputRefsForFilter = ImmutableList.of(1);

        // ($1 mod 3) == 0L
        predicate = buildCondition(1,
            TddlOperatorTable.PERCENT_REMAINDER, 3L,
            TddlOperatorTable.EQUALS, 0L
        );

        // NOTE: The inputRefs in RexNode must be in consistent with inputRefForFilter.
        evaluator = DefaultLazyEvaluator.builder()
            .setContext(context)
            .setRexNode(predicate)
            .setRatio(RATIO)
            .setInputTypes(ImmutableList.of(
                DataTypes.LongType, DataTypes.LongType, new VarcharType(), new VarcharType())) // input type: bigint.
            .build();

        String digest = VectorizedExpressionUtils.digest(evaluator.getCondition());
        System.out.println(digest);

        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(4, fromRowGroupIds(4, new int[] {2, 3}));
        doTest(
            matrix,
            ImmutableList.of(0, 2, 3)
        );
    }

    @Test
    public void test5() throws Throwable {
        // No evaluator
        inputRefsForFilter = ImmutableList.of();
        evaluator = null;

        SortedMap<Integer, boolean[]> matrix = new TreeMap<>();
        matrix.put(0, fromRowGroupIds(0, new int[] {0, 1, 2, 3, 4, 8, 10, 12, 14, 16, 18, 20, 21, 22}));
        matrix.put(1, fromRowGroupIds(1, new int[] {2, 4, 7, 11, 19, 20}));
        matrix.put(2, fromRowGroupIds(2, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(3, fromRowGroupIds(3, new int[] {0, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21}));
        matrix.put(4, fromRowGroupIds(4, new int[] {2, 3}));
        doTest(
            matrix,
            ImmutableList.of(0, 2, 3)
        );
    }

    public void doTest(
        SortedMap<Integer, boolean[]> rowGroupMatrix,
        List<Integer> inputRefsForProject
    )
        throws Throwable {
        context = new ExecutionContext();
        context.setTraceId(TRACE_ID);

        final int morselUnit = MORSEL_UNIT;

        NonBlockedScanPreProcessor preProcessor = new NonBlockedScanPreProcessor(
            preheatFileMeta, rowGroupMatrix, deletionBitmap
        );
        preProcessor.addFile(FILE_PATH);

        Set<Integer> refSet = new TreeSet<>();
        refSet.addAll(inputRefsForProject);
        refSet.addAll(inputRefsForFilter);
        List<ColumnMeta> columnMetas = refSet.stream().map(COLUMN_METAS::get).collect(Collectors.toList());
        List<Integer> locInOrc = refSet.stream().map(LOC_IN_ORC::get).collect(Collectors.toList());

        ColumnarSplit split = MorselColumnarSplit.newBuilder()
            .executionContext(context)
            .ioExecutor(IO_EXECUTOR)
            .fileSystem(FILESYSTEM, Engine.LOCAL_DISK)
            .configuration(CONFIGURATION)
            .sequenceId(SEQUENCE_ID)
            .file(FILE_PATH, FILE_ID)
            .columnTransformer(new OSSColumnTransformer(columnMetas, columnMetas, null, null, locInOrc))
            .inputRefs(inputRefsForFilter, inputRefsForProject)
            .cacheManager(blockCacheManager)
            .chunkLimit(DEFAULT_CHUNK_LIMIT)
            .morselUnit(morselUnit)
            .pushDown(evaluator)
            .prepare(preProcessor)
            .columnarManager(mockColumnarManager)
            .memoryAllocator(memoryAllocatorCtx)
            .build();

        // Check data consistency of orc vector and block
        Checker checker = new Checker(inputRefsForProject);

        ScanWork<ColumnarSplit, Chunk> scanWork;
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
                    }

                    // stripe = ...
                    // rgMatrix = ...
                    // proj_column = ...
                    // filter = ...
                    check(checker, result, scanRange.getStripeId());
                    break;
                }
                case FINISHED:
                    while ((result = ioStatus.popResult()) != null) {
                        check(checker, result, scanRange.getStripeId());
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

        checker.close();
    }

    /**
     * Encapsulate some inner state of orc-reader
     * and check each chunk after align with vectorized-batch.
     */
    private class Checker implements Closeable {
        private Reader reader;
        private RecordReader rows;
        private int stripeId;
        private TypeDescription projectSchema;

        public Checker(List<Integer> refsForProject) {
            reader = null;
            rows = null;
            stripeId = -1;

            projectSchema = TypeDescription.createStruct();
            for (int ref : refsForProject) {
                final int colId = ref + 1;
                switch (colId) {
                case 1:
                    projectSchema.addField("pk", TypeDescription.createLong());
                    break;
                case 2:
                    projectSchema.addField("i0", TypeDescription.createLong());
                    break;
                case 3:
                    projectSchema.addField("order_id", TypeDescription.createString());
                    break;
                case 4:
                    projectSchema.addField("v0", TypeDescription.createString());
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
            }
        }

        void init() throws IOException {
            // count the total rows before this stripe.
            StripeInformation stripeInformation = orcTail.getStripes().get(stripeId);
            Path path = new Path(getFileFromClasspath
                (TEST_ORC_FILE_NAME));

            Reader.Options options = new Reader.Options(CONFIGURATION).schema(projectSchema)
                .range(stripeInformation.getOffset(), stripeInformation.getLength());

            reader = OrcFile.createReader(path, OrcFile.readerOptions(CONFIGURATION));
            rows = reader.rows(options);
        }

        void seek(int rowId) throws IOException {
            rows.seekToRow(rowId);
        }

        void updatePosition(int stripeId, int rowId) throws IOException {
            if (stripeId != this.stripeId) {
                this.stripeId = stripeId;
                init();
            }

            seek(rowId);
        }

        void checkNext(Chunk targetChunk) throws IOException {
            VectorizedRowBatch batch = projectSchema.createRowBatch(DEFAULT_CHUNK_LIMIT);
            rows.nextBatch(batch);

            // check chunk rows.
            // Assert.assertEquals(batch.size, targetChunk.getPositionCount());

            for (int ref = 0; ref < targetChunk.getBlockCount(); ref++) {
                ColumnVector vector = batch.cols[ref];
                Block block = targetChunk.getBlock(ref);

                for (int row = 0; row < batch.size; row++) {
                    if (vector.isNull[row]) {
                        Assert.assertTrue(block.isNull(row));
                    } else {
                        // check non null
                        StringBuilder builder = new StringBuilder();
                        vector.stringifyValue(builder, row);

                        String expect = builder.toString();
                        if (expect.startsWith("\"")) {
                            // Check varchar column
                            Assert.assertEquals(
                                "\"" + ((Slice) block.getObject(row)).toStringUtf8() + "\"",
                                expect);
                        } else {
                            // Check long column
                            Assert.assertEquals(
                                expect,
                                String.valueOf(block.getObject(row))
                            );
                        }

                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
            if (rows != null) {
                rows.close();
            }
        }
    }

    private void check(Checker checker, Chunk chunk, int stripeId) throws IOException {
        Object firstValue = chunk.getBlock(0).getObject(0);
        Preconditions.checkArgument(firstValue instanceof Long);

        int rowId = (int) ((Long) firstValue - 1_000_000L);
        checker.updatePosition(stripeId, rowId);
        // checker.checkNext(chunk);

        System.out.println(MessageFormat.format(
            "Success to check the stripeId = {0} and row_id = {1}",
            stripeId, rowId
        ));
    }

    private RoaringBitmap buildDeletionBitmap(int markedCount) {
        RoaringBitmap result = new RoaringBitmap();
        for (int i = 0; i < markedCount * 2; i += 2) {
            result.add(i);
        }
        return result;
    }

    // (col op1 const1) op2 const2
    // for example:
    // ($2 + 1000) >= 150000
    private RexNode buildCondition(int inputRefIndex,
                                   SqlOperator op1, long const1,
                                   SqlOperator op2, long const2) {
        // column a and literal 1
        RexInputRef inputRef = REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), inputRefIndex);
        RexLiteral literal = REX_BUILDER.makeLiteral(const1, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT);

        // call: a+1
        RexNode plus = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            op1,
            ImmutableList.of(
                // column
                inputRef,
                // const
                literal
            )
        );

        // call: (a+1) >= 1000
        RexNode predicate = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            op2,
            ImmutableList.of(
                // column
                plus,
                // const
                REX_BUILDER.makeLiteral(const2, TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), BIGINT)
            )
        );
        return predicate;
    }
}

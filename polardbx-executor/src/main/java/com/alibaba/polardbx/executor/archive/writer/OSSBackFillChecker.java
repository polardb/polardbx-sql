package com.alibaba.polardbx.executor.archive.writer;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.BlockBuilders;
import com.alibaba.polardbx.executor.chunk.IXRowChunk;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.operator.ResultSetCursorExec;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.SqlSelect;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;

import java.io.Closeable;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OSSBackFillChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger("oss");

    private String sourceLogicalSchemaName;
    private String sourceLogicalTableName;
    private String loadTablePhysicalSchemaName;
    private String loadTablePhysicalTableName;

    private String localFilePath;
    private Configuration conf;
    private boolean[] noRedundantSchema;
    private int redundantId;
    private List<DataType> dataTypes;
    private List<ColumnProvider> columnProviders;

    private String physicalPartitionName;
    private Row lowRow;
    private Row upperRow;

    public OSSBackFillChecker(String sourceLogicalSchemaName, String sourceLogicalTableName,
                              String loadTablePhysicalSchemaName, String loadTablePhysicalTableName,
                              String localFilePath, Configuration conf, boolean[] noRedundantSchema, int redundantId,
                              List<DataType> dataTypes,
                              List<ColumnProvider> columnProviders, String physicalPartitionName,
                              Row lowRow, Row upperRow) {
        this.sourceLogicalSchemaName = sourceLogicalSchemaName;
        this.sourceLogicalTableName = sourceLogicalTableName;
        this.loadTablePhysicalSchemaName = loadTablePhysicalSchemaName;
        this.loadTablePhysicalTableName = loadTablePhysicalTableName;
        this.localFilePath = localFilePath;
        this.conf = conf;
        this.noRedundantSchema = noRedundantSchema;
        this.redundantId = redundantId;
        this.dataTypes = dataTypes;
        this.columnProviders = columnProviders;
        this.physicalPartitionName = physicalPartitionName;
        this.lowRow = lowRow;
        this.upperRow = upperRow;
    }

    /**
     * check whether the orc file is right or not
     */
    boolean checkTable(int fileIndex, ExecutionContext ec) {
        // read orc files
        Path path = new Path(localFilePath);

        Reader.Options readerOptions = new Reader.Options(conf).include(noRedundantSchema);

        SessionProperties sessionProperties = SessionProperties.fromExecutionContext(ec);

        try (Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(conf));
            RecordReader rows = reader.rows(readerOptions);
            DBCursor cursor = new DBCursor(
                ec, fileIndex)) {

            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            Row row = null;
            while (rows.nextBatch(batch)) {
                if (batch.size == 0) {
                    continue;
                }
                // read db files and build blocks
                Block[] dbBlocks = new Block[redundantId - 1];
                BlockBuilder[] dbBlockBuilder = createBlockBuilder(ec, batch.size);
                int blockLen = 0;
                while (blockLen < batch.size) {
                    row = cursor.next(row);
                    // build blocks
                    if (row instanceof ResultSetRow) {
                        ResultSet rs = ((ResultSetRow) row).getResultSet();
                        ResultSetCursorExec
                            .buildOneRow(rs, dataTypes.stream().toArray(DataType[]::new),
                                dbBlockBuilder);
                    } else if (row instanceof IXRowChunk) {
                        // XResult and deal with new interface.
                        ((IXRowChunk) row)
                            .buildChunkRow(dataTypes.stream().toArray(DataType[]::new),
                                dbBlockBuilder);
                    } else {
                        ResultSetCursorExec
                            .buildOneRow(row, dataTypes.stream().toArray(DataType[]::new),
                                dbBlockBuilder);
                    }
                    blockLen++;
                }
                for (int columnId = 1; columnId < redundantId; columnId++) {
                    dbBlocks[columnId - 1] = dbBlockBuilder[columnId - 1].build();
                }

                // read local orc file and build orc blocks
                Block[] orcBlocks = new Block[redundantId - 1];
                for (int columnId = 1; columnId < redundantId; columnId++) {
                    DataType dataType = dataTypes.get(columnId - 1);
                    BlockBuilder blockBuilder = BlockBuilders.create(dataType, ec);
                    ColumnVector columnVector = batch.cols[columnId - 1];
                    columnProviders.get(columnId - 1).transform(
                        columnVector,
                        blockBuilder,
                        0,
                        batch.size,
                        sessionProperties
                    );
                    orcBlocks[columnId - 1] = blockBuilder.build();
                }

                // where we actually test the table
                for (int columnId = 1; columnId < redundantId; columnId++) {
                    for (int r = 0; r < batch.size; ++r) {
                        if (!orcBlocks[columnId - 1].equals(r, dbBlocks[columnId - 1], r)) {
                            return handleError(dbBlocks, orcBlocks, columnId, r);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        return true;
    }

    /**
     * create the block builder for all columns
     *
     * @param ec ExecutionContext
     * @param len the len of each builder
     * @return an array of block builder
     */
    private BlockBuilder[] createBlockBuilder(ExecutionContext ec, int len) {
        BlockBuilder[] builders = new BlockBuilder[this.redundantId - 1];
        for (int columnId = 1; columnId < this.redundantId; columnId++) {
            builders[columnId - 1] = BlockBuilders.create(dataTypes.get(columnId - 1), ec, len);
        }
        return builders;
    }

    private boolean handleError(Block[] dbBlocks, Block[] orcBlocks, int columnId, int r) {
        StringBuilder sb = new StringBuilder();
        sb.append("\ndifference happens at the " + columnId + "-th column\n");
        sb.append("origin is: ")
            .append(getColumnValue(dbBlocks, columnId - 1, r)).append("\n");
        sb.append("orc    is: ")
            .append(getColumnValue(orcBlocks, columnId - 1, r)).append("\n");
        sb.append("origin row is: ");
        for (int id = 1; id < redundantId; id++) {
            sb.append(getColumnValue(dbBlocks, id - 1, r)).append(" ");
        }
        sb.append("\norc    row is: ");
        for (int id = 1; id < redundantId; id++) {
            sb.append(getColumnValue(orcBlocks, id - 1, r)).append(" ");
        }
        LOGGER.error(sb.toString());
        throw new TddlRuntimeException(ErrorCode.ERR_BACK_FILL_CHECK, sb.toString());
    }

    /**
     * transform block content to human friendly content
     *
     * @param blocks blocks of all columns
     * @param column the column id
     * @param row the row id
     * @return the readable content
     */
    private String getColumnValue(Block[] blocks, int column, int row) {
        Object obj = blocks[column].getObject(row);
        if (obj == null) {
            return null;
        }
        if (obj instanceof Slice) {
            return ((Slice) obj).toStringUtf8();
        } else
        if (obj instanceof byte[]) {
            return new String((byte[])obj);
        } else {
            return obj.toString();
        }
    }

    private class DBCursor implements Closeable {
        Extractor.ExtractorInfo info;
        long batchSize;
        PhyTableOperation planWithoutLower;
        PhyTableOperation plan;
        Cursor extractCursor;
        ExecutionContext ec;
        int fileIndex;

        DBCursor(ExecutionContext ec, int fileIndex) {

            info = Extractor.buildExtractorInfo(ec, sourceLogicalSchemaName,
                sourceLogicalTableName, sourceLogicalTableName);
            batchSize = ec.getParamManager().getLong(ConnectionParams.CHECK_OSS_BATCH_SIZE);
            // build the plan
            final PhysicalPlanBuilder builder =
                new PhysicalPlanBuilder(sourceLogicalSchemaName, ec);
            planWithoutLower = new PhyTableOperation(
                builder.buildSelectForBackfill(info, false, true,
                    SqlSelect.LockMode.SHARED_LOCK, physicalPartitionName));

            plan = new PhyTableOperation(
                builder.buildSelectForBackfill(info, true, true,
                    SqlSelect.LockMode.SHARED_LOCK, physicalPartitionName));
            this.ec = ec;
            extractCursor = null;
            this.fileIndex = fileIndex;
        }

        Row next(Row lastRow) {
            Row row = null;
            // generate next row
            if (extractCursor != null) {
                row = extractCursor.next();
            }
            // cursor ends
            if (row == null) {
                // last row in batch ends
                if (extractCursor != null) {
                    extractCursor.close(new ArrayList<>());
                }
                Row lowerBound = lastRow == null ? lowRow : lastRow;
                extractCursor = getBatchCursor(lowerBound == null ? planWithoutLower : plan,
                    info, batchSize, ec, lowerBound);
                row = extractCursor.next();
                // database ends but orc has more, shouldn't happen
                if (row == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_BACK_FILL_CHECK,
                        "record in database is less than orc!");
                }
            }
            return row;
        }

        Cursor getBatchCursor(PhyTableOperation plan, Extractor.ExtractorInfo info, long batchSize,
                              ExecutionContext ec, Row lastRow) {
            Map<Integer, ParameterContext> planParams = new HashMap<Integer, ParameterContext>();
            // Physical table is 1st parameter
            planParams.put(1, PlannerUtils
                .buildParameterContextForTableName(loadTablePhysicalTableName, 1));

            int nextParamIndex = 2;

            // Parameters for where(DNF)
            for (Row row : new Row[] {lastRow, upperRow}) {
                if (row != null) {
                    List<ParameterContext> firstPk = info.getPrimaryKeysId().stream()
                        .map(i -> Transformer.buildColumnParam(row, i)).collect(Collectors.toList());
                    for (int i = 0; i < firstPk.size(); i++) {
                        for (int j = 0; j <= i; j++) {
                            planParams.put(nextParamIndex,
                                new ParameterContext(firstPk.get(j).getParameterMethod(),
                                    new Object[] {nextParamIndex, firstPk.get(j).getArgs()[1]}));
                            nextParamIndex++;
                        }
                    }
                }
            }
            //batch size
            planParams.put(nextParamIndex,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {nextParamIndex, batchSize}));

            plan.setDbIndex(loadTablePhysicalSchemaName);
            plan.setTableNames(
                ImmutableList.of(ImmutableList.of(loadTablePhysicalTableName)));
            plan.setParam(planParams);
            return ExecutorHelper.execute(plan, ec);
        }

        @Override
        public void close() throws IOException {
            if (extractCursor != null) {
                extractCursor.close(new ArrayList<>());
            }
        }
    }
}
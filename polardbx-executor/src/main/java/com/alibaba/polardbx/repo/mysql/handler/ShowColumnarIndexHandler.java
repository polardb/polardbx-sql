package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.JdbcUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowColumnarIndex;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class ShowColumnarIndexHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(ShowColumnarIndexHandler.class);

    public ShowColumnarIndexHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor resultCursor = buildResultCursor();

        SqlShowColumnarIndex showGlobalIndex = (SqlShowColumnarIndex) ((LogicalDal) logicalPlan).getNativeSqlNode();
        SqlIdentifier tableNameNode = (SqlIdentifier) showGlobalIndex.getTable();
        String schemaName = (tableNameNode != null && 2 == tableNameNode.names.size()) ? tableNameNode.names.get(0) :
            executionContext.getSchemaName();
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (null == executionContext) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }

        GsiMetaManager metaManager = executorContext.getGsiManager().getGsiMetaManager();

        GsiMetaManager.GsiMetaBean meta;
        if (null == tableNameNode) {
            meta = metaManager.getAllGsiMetaBean(schemaName);
        } else {
            String tableName = tableNameNode.getLastName();
            SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
            TableMeta tableMeta = sm.getTableWithNull(tableName);
            if (tableMeta == null) {
                throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
            }
            meta = metaManager.getTableAndIndexMeta(tableName, IndexStatus.ALL);
        }

        SchemaManager schemaManager = executionContext.getSchemaManager();
        Connection metaDbConn = null;
        try {
            try {
                metaDbConn = MetaDbDataSource.getInstance().getConnection();
            } catch (Exception e) {
                // ignore
                logger.warn("failed to get metadb connection: ", e);
            }
            for (GsiMetaManager.GsiTableMetaBean bean : meta.getTableMeta().values()) {
                if (bean.gsiMetaBean != null && bean.gsiMetaBean.columnarIndex) {
                    GsiMetaManager.GsiIndexMetaBean bean1 = bean.gsiMetaBean;
                    String pkNames =
                        bean1.indexColumns.stream().map(col -> col.columnName).collect(Collectors.joining(", "));
                    String partitionKeyNames = pkNames;
                    String coveringNames =
                        bean1.coveringColumns.stream().map(col -> col.columnName).collect(Collectors.joining(", "));
                    String partitionStrategy = "";
                    Integer partitionCount = null;
                    TableMeta tableMeta = schemaManager.getTable(bean1.indexName);
                    if (tableMeta != null && tableMeta.getPartitionInfo() != null) {
                        PartitionByDefinition partitionBy = tableMeta.getPartitionInfo().getPartitionBy();
                        partitionStrategy = partitionBy.getStrategy().name();
                        partitionCount = partitionBy.getPartitions().size();
                        partitionKeyNames = String.join(", ", partitionBy.getPartitionColumnNameList());
                    }

                    String options = null;
                    if (metaDbConn != null) {
                        try {
                            options = getColumnarIndexOptions(metaDbConn, bean1.tableSchema, bean1.indexName);
                        } catch (Exception e) {
                            // ignore
                            logger.warn("failed to query columnar option info", e);
                        }
                    }

                    Object[] row = new Object[] {
                        bean1.tableSchema, bean1.tableName, bean1.indexName, Boolean.toString(bean1.clusteredIndex),
                        pkNames, coveringNames, partitionKeyNames, partitionStrategy, partitionCount,
                        pkNames, options, bean1.indexStatus};
                    resultCursor.addRow(row);
                }
            }
        } finally {
            JdbcUtils.close(metaDbConn);
        }

        return resultCursor;
    }

    private String getColumnarIndexOptions(Connection metaDbConn,
                                           String schemaName,
                                           String indexName) {
        ColumnarTableEvolutionAccessor accessor = new ColumnarTableEvolutionAccessor();
        accessor.setConnection(metaDbConn);
        List<ColumnarTableEvolutionRecord> records =
            accessor.querySchemaIndexLatest(schemaName, indexName);
        if (CollectionUtils.isEmpty(records)) {
            logger.warn("empty columnar_table_evolution record: " + indexName);
            return null;
        }
        Map<String, String> options = records.get(0).options;
        return ColumnarTableEvolutionRecord.serializeToJson(options);
    }

    private ArrayResultCursor buildResultCursor() {

        ArrayResultCursor resultCursor = new ArrayResultCursor("COLUMNAR_INDEXES");

        resultCursor.addColumn("SCHEMA", DataTypes.StringType);
        resultCursor.addColumn("TABLE", DataTypes.StringType);
        resultCursor.addColumn("INDEX_NAME", DataTypes.StringType);
        resultCursor.addColumn("CLUSTERED", DataTypes.StringType);
        resultCursor.addColumn("PK_NAMES", DataTypes.StringType);
        resultCursor.addColumn("COVERING_NAMES", DataTypes.StringType);
        resultCursor.addColumn("PARTITION_KEY", DataTypes.StringType);
        resultCursor.addColumn("PARTITION_STRATEGY", DataTypes.StringType);
        resultCursor.addColumn("PARTITION_COUNT", DataTypes.IntegerType);
        resultCursor.addColumn("SORT_KEY", DataTypes.StringType);
        resultCursor.addColumn("OPTIONS", DataTypes.StringType);
        resultCursor.addColumn("STATUS", DataTypes.StringType);

        resultCursor.initMeta();

        return resultCursor;
    }

}

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartitionTupleRoutingContext {

    /**
     * dbName
     */
    protected String schemaName;

    /**
     * Logical table name
     */
    protected String tableName;

    /**
     * the partInfo of tableName
     */
    protected PartitionInfo partInfo;

    /**
     * the targetRowColMetas of target values to be routing
     */
    private List<ColumnMeta> targetRowColMetas;

    /**
     * the partition column metas, its order is important!
     */
    protected List<ColumnMeta> partColMetas;

    /**
     * the relRowDataType of partition columns based on the targetRowColMetas of target values
     */
    protected RelDataType partColRelRowType;

    /**
     * the index of  each partition columns in the targetRowColMetas, its order is important!
     * <p>
     * its order must be the same as the partition columns definitions.
     * <p>
     * <pre>
     *
     *  For example:
     *      target col metas : (b,c,d,a)
     *      target values : (1,100,1000,10000)
     *      partition columns:  (a,b)
     *      column index of target col metas: (1,2,3,4)
     *      column index of partition col metas: (4,1)
     *      target values of partition col metas: (10000,1)
     * </pre>
     */
    protected List<Integer> partColIndexMappings = new ArrayList<>();

    protected PartitionTupleRoutingContext() {
    }

    public SqlCall createPartColDynamicParamAst() {
        SqlNode[] rowOpArr = new SqlNode[this.partColMetas.size()];
        SqlBasicCall rowAst = new SqlBasicCall(TddlOperatorTable.ROW, rowOpArr, SqlParserPos.ZERO);
        for (int i = 0; i < rowOpArr.length; i++) {
            rowOpArr[i] = new SqlDynamicParam(i, SqlParserPos.ZERO);
        }
        SqlNode[] rowsAstOpArr = new SqlNode[1];
        rowsAstOpArr[0] = rowAst;
        return new SqlBasicCall(TddlOperatorTable.VALUES, rowsAstOpArr, SqlParserPos.ZERO);
    }

    public Parameters createPartColValueParameters(List<Object> targetRowValues) {
        return createPartColValueParameters(targetRowValues, true);
    }

    public Parameters createPartColValueParameters(List<Object> targetRowValues, boolean usePartColIndexMapping) {
        Map<Integer, ParameterContext> tmpParams = new HashMap<>();
        int partColCnt = partInfo.getPartitionBy().getPartitionColumnNameList().size();
        for (int j = 0; j < partColCnt; j++) {
            Object val = null;
            if (usePartColIndexMapping) {
                Integer idxInTargetRowColMetas = partColIndexMappings.get(j);
                val = targetRowValues.get(idxInTargetRowColMetas);
            } else {
                val = targetRowValues.get(j);
            }
            ParameterContext pc = new ParameterContext(ParameterMethod.setObject1, new Object[] {j + 1, val});
            tmpParams.put(j + 1, pc);
        }
        Parameters allParams = new Parameters(tmpParams);
        return allParams;
    }

    protected void computePartColIndexMappings(List<Integer> outputPartColIdxList,
                                               List<RelDataTypeField> outputPartColRelRowTypeList) {
        for (ColumnMeta columnMeta : partColMetas) {
            String partColName = columnMeta.getName();
            if (partColName.contains(".")) {
                partColName = partColName.split("\\.")[1]; // 避免转义
            }

            List<ColumnMeta> targetColumnList = targetRowColMetas;
            int index = -1;
            for (int i = 0; i < targetColumnList.size(); i++) {
                String colName = targetColumnList.get(i).getField().getOriginColumnName();
                if (colName.equalsIgnoreCase(partColName)) {
                    index = i;
                    break;
                }
            }
            // if it's absent, it's using default value
            if (index < 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL_UNKNOWN_COLUMN,
                    "No found any partition column of " + partColName);
            }
            outputPartColIdxList.add(index);
            RelDataTypeField relDataTypeField =
                new RelDataTypeFieldImpl(columnMeta.getName(), index, columnMeta.getField().getRelType());
            outputPartColRelRowTypeList.add(relDataTypeField);
        }
    }

    protected void initTupleRoutingContext() {
        this.partColMetas = partInfo.getPartitionBy().getPartitionFieldList();
        List<Integer> partColIdxList = new ArrayList<>();
        List<RelDataTypeField> outputPartColRelRowTypeList = new ArrayList<>();
        computePartColIndexMappings(partColIdxList, outputPartColRelRowTypeList);
        partColIndexMappings = partColIdxList;
        partColRelRowType = new RelRecordType(outputPartColRelRowTypeList);
    }

    public static PartitionTupleRoutingContext buildPartitionTupleRoutingContext(String schemaName,
                                                                                 String tableName,
                                                                                 PartitionInfo partInfo,
                                                                                 List<ColumnMeta> targetValuesColMetas) {
        PartitionTupleRoutingContext ctx = new PartitionTupleRoutingContext();
        ctx.schemaName = schemaName;
        ctx.tableName = tableName;
        ctx.partInfo = partInfo;
        ctx.targetRowColMetas = targetValuesColMetas;
        ctx.initTupleRoutingContext();
        return ctx;
    }

    public RelDataType getPartColRelRowType() {
        return partColRelRowType;
    }

    public List<Integer> getPartColIndexMappings() {
        return partColIndexMappings;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionInfo getPartInfo() {
        return partInfo;
    }
}

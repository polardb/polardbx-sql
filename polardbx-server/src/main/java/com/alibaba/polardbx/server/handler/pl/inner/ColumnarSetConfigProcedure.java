package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarConfigRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author lijiu
 */
public class ColumnarSetConfigProcedure extends BaseInnerProcedure {

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        List<SQLExpr> params = statement.getParameters();
        //参数解析
        List<Object> parameters = checkParameters(params, statement);

        //设置列存参数
        List<Object> result = setColumnarConfig(parameters);

        if (result.size() == 3) {
            cursor.addColumn("index_id", DataTypes.LongType);
            cursor.addColumn("key", DataTypes.StringType);
            cursor.addColumn("value", DataTypes.StringType);
            cursor.addRow(new Object[] {result.get(0), result.get(1), result.get(2)});
        } else if (result.size() == 2) {
            cursor.addColumn("key", DataTypes.StringType);
            cursor.addColumn("value", DataTypes.StringType);
            cursor.addRow(new Object[] {result.get(0), result.get(1)});
        } else if (result.size() == 6) {
            cursor.addColumn("schema_name", DataTypes.StringType);
            cursor.addColumn("table_name", DataTypes.StringType);
            cursor.addColumn("index_name", DataTypes.StringType);
            cursor.addColumn("index_id", DataTypes.LongType);
            cursor.addColumn("key", DataTypes.StringType);
            cursor.addColumn("value", DataTypes.StringType);
            cursor.addRow(new Object[] {
                result.get(0), result.get(1), result.get(2), result.get(3), result.get(4),
                result.get(5)});
        }
    }

    /**
     * 3个参数时，第一个是table id，代表设置表级别；后面两个是key，value
     * 2个参数时，代表实例级别；后面两个是key，value
     * 5个参数时，代表设置某个索引的参数，schema、table、index、key、value
     */
    private List<Object> checkParameters(List<SQLExpr> params, SQLCallStatement statement) {
        //参数不是两个，或者3个，5个报错
        if (!(params.size() == 2 || params.size() == 3 || params.size() == 5)) {
            throw new IllegalArgumentException(statement.toString() + " parameters is not match 2/3/5 parameters");
        }
        if (params.size() == 3 && !(params.get(0) instanceof SQLIntegerExpr)) {
            throw new IllegalArgumentException(
                statement.toString() + " first parameters need Long number when hava 3 parameters");
        }
        if (params.size() == 5
            && (!(params.get(0) instanceof SQLCharExpr)
            || !(params.get(1) instanceof SQLCharExpr)
            || !(params.get(2) instanceof SQLCharExpr))) {
            throw new IllegalArgumentException(
                statement.toString() + " first/second/third parameters need String when hava 5 parameters");
        }
        //默认0是实例级别
        SQLExpr key;
        SQLExpr value;
        if (params.size() == 3) {
            key = params.get(1);
            value = params.get(2);
        } else if (params.size() == 2) {
            key = params.get(0);
            value = params.get(1);
        } else {
            key = params.get(3);
            value = params.get(4);
        }

        if (!(key instanceof SQLCharExpr)) {
            throw new IllegalArgumentException(statement.toString() + " key parameters need String");
        }

        String setKey = ((SQLCharExpr) key).getText();
        String setValue;

        if (value instanceof SQLCharExpr) {
            setValue = ((SQLCharExpr) value).getText();
        } else if (value instanceof SQLNumericLiteralExpr) {
            setValue = String.valueOf(((SQLNumericLiteralExpr) value).getNumber());
        } else if (value instanceof SQLBooleanExpr) {
            setValue = String.valueOf(((SQLBooleanExpr) value).getBooleanValue());
        } else {
            throw new IllegalArgumentException(value + " value parameters need String");
        }

        List<Object> result = new ArrayList<>();

        if (params.size() == 3) {
            long tableId = ((SQLIntegerExpr) params.get(0)).getNumber().longValue();
            result.add(tableId);
        } else if (params.size() == 5) {
            String schema_name = ((SQLCharExpr) params.get(0)).getText();
            String table_name = ((SQLCharExpr) params.get(1)).getText();
            String index_name = ((SQLCharExpr) params.get(2)).getText();
            result.add(schema_name);
            result.add(table_name);
            result.add(index_name);
        }

        result.add(setKey);
        result.add(setValue);

        return result;
    }

    private List<Object> setColumnarConfig(List<Object> parameters) {
        List<Object> result = parameters;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarConfigAccessor accessor = new ColumnarConfigAccessor();
            accessor.setConnection(metaDbConn);
            ColumnarConfigRecord record = new ColumnarConfigRecord();
            if (parameters.size() == 3) {
                record.tableId = (long) parameters.get(0);
                record.configKey = (String) parameters.get(1);
                record.configValue = (String) parameters.get(2);
            } else if (parameters.size() == 2) {
                record.configKey = (String) parameters.get(0);
                record.configValue = (String) parameters.get(1);
            } else if (parameters.size() == 5) {
                ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
                tableMappingAccessor.setConnection(metaDbConn);
                String schemaName = (String) parameters.get(0);
                String tableName = (String) parameters.get(1);
                String indexName = (String) parameters.get(2) + '%';
                List<ColumnarTableMappingRecord> records = tableMappingAccessor.queryBySchemaTableIndexLike(schemaName,
                    tableName, indexName, ColumnarTableStatus.PUBLIC.name());
                if (records.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format(" %s.%s index like %s not found columnar index", schemaName, tableName,
                            indexName));
                }
                if (records.size() >= 2) {
                    throw new IllegalArgumentException(
                        String.format(" %s.%s index %s found more than one columnar index[%s]", schemaName, tableName,
                            indexName, records.stream().map(r -> r.indexName).collect(Collectors.joining(","))));
                }
                record.tableId = records.get(0).tableId;
                record.configKey = (String) parameters.get(3);
                record.configValue = (String) parameters.get(4);

                result = new ArrayList<>(6);
                result.add(records.get(0).tableSchema);
                result.add(records.get(0).tableName);
                result.add(records.get(0).indexName);
                result.add(records.get(0).tableId);
                result.add(record.configKey);
                result.add(record.configValue);
            }
            accessor.insert(ImmutableList.of(record));
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

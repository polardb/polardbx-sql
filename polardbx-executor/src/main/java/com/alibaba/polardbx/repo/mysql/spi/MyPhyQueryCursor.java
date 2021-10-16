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

package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.druid.proxy.jdbc.ResultSetMetaDataProxy;
import com.mysql.cj.polarx.protobuf.PolarxResultset;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.compatible.XResultSetMetaData;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.biv.MockResultSetMetaData;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalSet;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class MyPhyQueryCursor extends ResultCursor {

    public static final java.lang.reflect.Field MYSQL_RSMD_FIELDS;

    static {
        try {
            MYSQL_RSMD_FIELDS = com.mysql.jdbc.ResultSetMetaData.class.getDeclaredField("fields");
            MYSQL_RSMD_FIELDS.setAccessible(true);
        } catch (SecurityException | NoSuchFieldException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    protected MyJdbcHandler handler;
    protected BaseQueryOperation plan;
    protected CursorMeta meta;
    protected MyRepository repo;
    protected ExecutionContext ec;
    protected int[] affectRows;

    public MyPhyQueryCursor(ExecutionContext ec, BaseQueryOperation logicalPlan, MyRepository repo) {

        super();
        this.plan = logicalPlan;
        this.repo = repo;
        this.ec = ec;
        this.handler = repo.createQueryHandler(ec);
        init();
    }

    @Override
    public void doInit() {
        if (inited) {
            return;
        }

        try {
            if (useUpdate()) {
                forbidPushDmlWithHintIfNeed();
                this.meta = CalciteUtils.buildDmlCursorMeta();
                this.returnColumns = meta.getColumns();
                this.affectRows = handler.executeUpdate(plan);
            } else {
                handler.executeQuery(meta, plan);
                buildReturnColumns();
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
        inited = true;
    }

    private boolean useUpdate() {
        return plan instanceof LogicalSet || plan.getKind().belongsTo(SqlKind.SQL_SET_QUERY)
            || plan.getKind().belongsTo(SqlKind.DML) || plan.getKind().belongsTo(SqlKind.DDL);
    }

    private void forbidPushDmlWithHintIfNeed() {
        ExecutionContext executionContext = this.handler.getExecutionContext();
        if (executionContext.isOriginSqlPushdownOrRoute()) {
            String groupName = this.plan.getDbIndex();
            if (!groupName
                .equalsIgnoreCase(MetaDbDataSource.DEFAULT_META_DB_GROUP_NAME)) {
                if (HintUtil.forbidPushDmlWithHint()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Unsupported to push physical dml by hint");
                }

            }
        }
    }

    @Override
    public Row doNext() {
        try {
            init();

            if (useUpdate()) {
                ArrayRow arrayRow = new ArrayRow(1, this.meta);
                arrayRow.setObject(0, this.affectRows[0]);
                arrayRow.setCursorMeta(meta);
                return arrayRow;
            } else {
                Row next = handler.next();
                if (null != next) {
                    next.setCursorMeta(meta);
                }
                return next;
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exs) {
        if (exs == null) {
            exs = new ArrayList<>();
        }
        try {
            handler.close();

        } catch (Exception e) {
            exs.add(new TddlException(e));
        }

        return exs;
    }

    private void buildReturnColumns() {
        if (returnColumns != null) {
            return;
        }

        // 使用meta做为returncolumns
        // resultset中返回的meta信是物理表名，会导致join在构造返回对象时找不到index(表名不同/为null)
        if (meta != null) {
            this.returnColumns = new ArrayList<>(meta.getColumns());
        } else {
            try {
                if (this.handler.getResultSet() == null) {
                    handler.executeQuery(meta, plan);
                }

                final ResultSetMetaData rsmd = this.handler.getResultSet().getMetaData();

                this.returnColumns = getReturnColumnMeta(rsmd);
                this.meta = CursorMeta.build(returnColumns);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }

    }

    static List<ColumnMeta> getReturnColumnMeta(ResultSetMetaData rsmd) {
        final List<ColumnMeta> returnColumns = new ArrayList<>();
        try {
            if (rsmd instanceof ResultSetMetaDataProxy) {
                rsmd = rsmd.unwrap(com.mysql.jdbc.ResultSetMetaData.class);
            }

            if (rsmd instanceof com.mysql.jdbc.ResultSetMetaData) {
                com.mysql.jdbc.Field[] fields = (com.mysql.jdbc.Field[]) MYSQL_RSMD_FIELDS.get(rsmd);
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String name = rsmd.getColumnLabel(i);
                    String tableName = fields[i - 1].getTableName();
                    String originTableName = fields[i - 1].getOriginalTableName();
                    long length = fields[i - 1].getLength();
                    int precision = rsmd.getPrecision(i);
                    int scale = rsmd.getScale(i);

                    int collationIndex = 0;
                    try {
                        Class<?> clazz = Class.forName("com.mysql.jdbc.Field");
                        java.lang.reflect.Field filed = clazz.getDeclaredField("collationIndex");
                        filed.setAccessible(true);
                        collationIndex = (int) filed.get(fields[i - 1]);
                    } catch (Throwable ignore) {
                    }

                    RelDataType calciteDataType = DataTypeUtil.jdbcTypeToRelDataType(rsmd.getColumnType(i),
                        rsmd.getColumnTypeName(i), precision, scale, length, true);
                    Field field = new Field(originTableName, name, collationIndex, null, null,
                        calciteDataType, false,
                        fields[i - 1].isPrimaryKey()
                    );
                    ColumnMeta columnMeta = new ColumnMeta(tableName, name, null, field);

                    returnColumns.add(columnMeta);
                }
            } else if (rsmd instanceof MockResultSetMetaData) {
                returnColumns.addAll(((MockResultSetMetaData) rsmd).getColumnMetas());
            } else if (rsmd instanceof XResultSetMetaData) {
                for (int i = 0; i < rsmd.getColumnCount(); ++i) {
                    final PolarxResultset.ColumnMetaData metaData =
                        ((XResultSetMetaData) rsmd).getResult().getMetaData().get(i);
                    returnColumns.add(TableMetaParser
                        .buildColumnMeta(metaData, XSession.toJavaEncoding(
                            ((XResultSetMetaData) rsmd).getResult().getSession()
                                .getResultMetaEncodingMySQL()),
                            null, null));
                }
            } else {
                throw new NotSupportException();
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }

        return returnColumns;
    }

    public int getAffectedRows() {
        return affectRows[0];
    }
}

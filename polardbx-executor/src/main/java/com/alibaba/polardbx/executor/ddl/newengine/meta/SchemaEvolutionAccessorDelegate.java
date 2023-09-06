package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class SchemaEvolutionAccessorDelegate<T> extends MetaDbAccessorWrapper<T> {

    protected static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    protected ColumnEvolutionAccessor columnEvolutionAccessor;

    protected ColumnMappingAccessor columnMappingAccessor;
    protected TablesAccessor tablesAccessor;
    Connection connection;

    public SchemaEvolutionAccessorDelegate() {
        this.columnEvolutionAccessor = new ColumnEvolutionAccessor();
        this.columnMappingAccessor = new ColumnMappingAccessor();
        this.tablesAccessor = new TablesAccessor();
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.columnEvolutionAccessor.setConnection(metaDbConn);
        this.columnMappingAccessor.setConnection(metaDbConn);
        this.tablesAccessor.setConnection(metaDbConn);
        this.connection = metaDbConn;
    }

    @Override
    public T execute() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                open(metaDbConn);
                MetaDbUtil.beginTransaction(metaDbConn);

                T r = invoke();

                MetaDbUtil.commit(metaDbConn);
                return r;
            } catch (Throwable t) {
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "change file storage metas");
                throw t;
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                close();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    @Override
    protected void close() {

    }
}
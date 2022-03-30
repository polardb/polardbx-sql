package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.scheduler.DdlPlanAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class DdlPlanAccessorDelegate<T> extends MetaDbAccessorWrapper<T> {

    protected static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    protected final DdlPlanAccessor ddlPlanAccessor;
    protected Connection connection;

    public DdlPlanAccessorDelegate() {
        this.ddlPlanAccessor = new DdlPlanAccessor();
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.ddlPlanAccessor.setConnection(metaDbConn);
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
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "remove table meta");
                onException(metaDbConn, t);
                throw t;
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                close();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    protected void onException(Connection metaDbConn, Throwable throwable) {
    }

    @Override
    protected void close() {
        this.ddlPlanAccessor.setConnection(null);
    }

    public Connection getConnection() {
        return this.connection;
    }
}

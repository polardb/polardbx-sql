package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class ScheduledJobsAccessorDelegate<T> extends MetaDbAccessorWrapper<T> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ScheduledJobsAccessorDelegate.class);

    protected ScheduledJobsAccessor scheduledJobsAccessor;
    protected FiredScheduledJobsAccessor firedScheduledJobsAccessor;
    protected Connection connection;

    public ScheduledJobsAccessorDelegate() {
        this.scheduledJobsAccessor = new ScheduledJobsAccessor();
        this.firedScheduledJobsAccessor = new FiredScheduledJobsAccessor();
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.connection = metaDbConn;
        this.scheduledJobsAccessor.setConnection(metaDbConn);
        this.firedScheduledJobsAccessor.setConnection(metaDbConn);
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
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "process scheduled jobs");
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
        this.scheduledJobsAccessor.setConnection(null);
        this.firedScheduledJobsAccessor.setConnection(null);
        this.connection = null;
    }

    public Connection getConnection() {
        return this.connection;
    }
}
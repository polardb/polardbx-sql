package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Action that move group between storage nodes
 *
 * @author moyi
 * @since 2021/05
 */
public class ActionDrainDatabase implements BalanceAction, Comparable<ActionDrainDatabase> {

    private String schema;
    private String sql;



    @JSONCreator
    public ActionDrainDatabase(String schema, String sql) {
        this.schema = schema;
        this.sql = sql;
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    @Override
    public String getName() {
        return "DrainDatabase";
    }

    @Override
    public String getStep() {
        return sql;
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        return ActionUtils.convertToDelegatorJob(ec, schema, sql);
    }

    public String getSql() {
        return sql;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionDrainDatabase)) {
            return false;
        }
        ActionDrainDatabase drainDatabase = (ActionDrainDatabase) o;
        return Objects.equals(sql, drainDatabase.getSql()) && Objects
            .equals(schema, drainDatabase.getSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, sql);
    }

    @Override
    public String toString() {
        return "ActionDrainDatabase{" +
            "schemaName=" + schema +
            ", sql=" + sql +
            '}';
    }

    @Override
    public int compareTo(ActionDrainDatabase o) {
        int res = schema.compareTo(o.schema);
        if (res != 0) {
            return res;
        }
        return  o.getSql().compareTo(sql);
    }
}

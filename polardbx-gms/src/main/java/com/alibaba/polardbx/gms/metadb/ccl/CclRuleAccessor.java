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

package com.alibaba.polardbx.gms.metadb.ccl;

import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2020/10/12 12:41 下午
 */
public class CclRuleAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CclRuleAccessor.class);

    private static final String CONCURRENCY_CONTROL_RULE_TABLE = wrap(GmsSystemTables.CONCURRENCY_CONTROL_RULE);

    private static final String INSERT_SQL_FILTER_RULES = "insert %s into "
        + CONCURRENCY_CONTROL_RULE_TABLE
        + "( `id`,`sql_type`, `db_name`, `table_name`, `user_name`, `client_ip`, `parallelism`, `keywords`, `template_id`, `query`, `params`, `query_template_id`, `queue_size`, `wait_timeout`, `fast_match`, `light_wait`, `trigger_priority`, `inst_id`)"
        + " values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String DELETE_BY_TRIGGER_PRIORITY_IN =
        "delete from " + CONCURRENCY_CONTROL_RULE_TABLE + " where trigger_priority in (%s)";

    private static final String SELECT_ALL = " select * from " + CONCURRENCY_CONTROL_RULE_TABLE;

    private static final String ORDER = " order by `priority` desc";

    private static final String WHERE_ID_IN_FORMAT = " where id in (%s)";

    private static final String WHERE_PRIORITY_IN_FORMAT = " where trigger_priority in (%s) ";

    private static final String AND_INST_ID = String.format(" and `inst_id`= '%s' ", InstIdUtil.getInstId());

    private static final String WHERE_INST_ID = String.format(" where `inst_id` = '%s' ", InstIdUtil.getInstId());

    private static final String DELETE_ALL = "delete from " + CONCURRENCY_CONTROL_RULE_TABLE;

    private static final String UPDATE_SQL_FILTER_RULE = "update " + CONCURRENCY_CONTROL_RULE_TABLE
        + " set `id`=?, `sql_type`=?, `db_name`=?, `table_name`=?, `user_name`=?, `client_ip`=?, `parallelism`=?, `keywords`=?, `template_id`=?, `query`=?, `params`=?, `query_template_id`=?, `queue_size`=?, `wait_timeout`=?, `fast_match`=?, `light_wait`=?, `trigger_priority`=?, `inst_id` = ?, `gmt_updated` = now() where `priority`=?";

    private static final String DELETE_BY_TRIGGER_PRIORITY =
        "delete from " + CONCURRENCY_CONTROL_RULE_TABLE + " where trigger_priority = ?";

    public int insert(CclRuleRecord record) {
        return insert(String.format(INSERT_SQL_FILTER_RULES, ""), CONCURRENCY_CONTROL_RULE_TABLE, record.buildParams());
    }

    public int insertIgnore(CclRuleRecord record) {
        return insert(String.format(INSERT_SQL_FILTER_RULES, "ignore"), CONCURRENCY_CONTROL_RULE_TABLE,
            record.buildParams());
    }

    public int deleteByIds(List<String> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return 0;
        }
        String sql = String.format(DELETE_ALL + WHERE_ID_IN_FORMAT + AND_INST_ID, concat(ids));
        return deleteBySql(sql);
    }

    public int deleteAll() {
        return deleteBySql(DELETE_ALL + WHERE_INST_ID);
    }

    public List<CclRuleRecord> queryByIds(List<String> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.EMPTY_LIST;
        }
        String sql = String.format(SELECT_ALL + WHERE_ID_IN_FORMAT + ORDER, concat(ids));
        List<CclRuleRecord> queryResult = queryBySql(sql);
        return queryResult;
    }

    public List<CclRuleRecord> queryByPriorities(List<Integer> priorities) {
        if (CollectionUtils.isEmpty(priorities)) {
            return Collections.EMPTY_LIST;
        }
        String sql = String.format(SELECT_ALL + WHERE_PRIORITY_IN_FORMAT + ORDER,
            concat(priorities.stream().map((e) -> e.toString()).collect(
                Collectors.toList())));
        List<CclRuleRecord> queryResult = queryBySql(sql);
        return queryResult;
    }

    public List<CclRuleRecord> query() {
        List<CclRuleRecord> queryResult = queryBySql(SELECT_ALL + WHERE_INST_ID + ORDER);
        return queryResult;
    }

    public int update(CclRuleRecord record) {
        Map<Integer, ParameterContext> params = record.buildParams();
        int size = params.size();
        MetaDbUtil.setParameter(size + 1, params, ParameterMethod.setLong, record.priority);
        int updatedRows = update(UPDATE_SQL_FILTER_RULE, GmsSystemTables.CONCURRENCY_CONTROL_RULE, params);
        return updatedRows;
    }

    private List<CclRuleRecord> queryBySql(String sql) {
        try {
            return MetaDbUtil.query(sql, CclRuleRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query system table " + CONCURRENCY_CONTROL_RULE_TABLE + " sql: " + sql, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                CONCURRENCY_CONTROL_RULE_TABLE,
                e.getMessage());
        }
    }

    public int deleteByTriggerPriority(int triggerPriority) {
        Map<Integer, ParameterContext> params = Maps.newHashMap();
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, triggerPriority);
        try {
            int deletedRows = MetaDbUtil.update(DELETE_BY_TRIGGER_PRIORITY, params, connection);
            return deletedRows;
        } catch (Exception e) {
            LOGGER.error(String.format("Failed to delete the ccl rules whose trigger priority is %d", triggerPriority),
                e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                CONCURRENCY_CONTROL_RULE_TABLE,
                e.getMessage());
        }
    }

    public int deleteByTriggers(List<Integer> triggerPriorities) {
        if (CollectionUtils.isEmpty(triggerPriorities)) {
            return 0;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < triggerPriorities.size(); ++i) {
            sb.append(triggerPriorities.get(i));
            if (i != triggerPriorities.size() - 1) {
                sb.append(",");
            }
        }
        String triggerPrioritiesStr = sb.toString();
        String sql = String.format(DELETE_BY_TRIGGER_PRIORITY_IN, triggerPrioritiesStr);
        try {
            int deletedRows = MetaDbUtil.execute(sql, Maps.newHashMap(), connection);
            return deletedRows;
        } catch (SQLException e) {
            LOGGER.error(String.format("Failed to delete the ccl rules whose trigger priorities is %s", sb.toString()),
                e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                CONCURRENCY_CONTROL_RULE_TABLE,
                e.getMessage());
        }
    }

    public void flush() {

    }

    private int deleteBySql(String sql) {
        try {
            return MetaDbUtil.delete(sql, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete system table " + CONCURRENCY_CONTROL_RULE_TABLE + " sql: " + sql, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                CONCURRENCY_CONTROL_RULE_TABLE,
                e.getMessage());
        }
    }

}

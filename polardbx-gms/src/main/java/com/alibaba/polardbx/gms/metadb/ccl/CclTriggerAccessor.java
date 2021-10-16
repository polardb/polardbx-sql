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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author busu
 * date: 2021/3/29 10:56 上午
 */
public class CclTriggerAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CclTriggerAccessor.class);
    private static final String CONCURRENCY_CONTROL_TRIGGER_TABLE = wrap(GmsSystemTables.CONCURRENCY_CONTROL_TRIGGER);

    private static final String INSERT_SQL_FILTER_TRIGGERS =
        "insert into " + CONCURRENCY_CONTROL_TRIGGER_TABLE
            + "(`id`, `priority`, `conditions`, `rule_config`, `schema`, `rule_upgrade`, `max_ccl_rule`, `ccl_rule_count`, `max_sql_size`, `inst_id`) values(?,?,?,?,?,?,?,?,?,?)";

    private static final String SELECT_ALL = " select * from " + CONCURRENCY_CONTROL_TRIGGER_TABLE;

    private static final String AND_INST_ID = String.format(" and `inst_id` = '%s' ", InstIdUtil.getInstId());

    private static final String WHERE_INST_ID = String.format(" where `inst_id` = '%s' ", InstIdUtil.getInstId());

    private static final String ORDER = " order by `priority` desc";

    private static final String WHERE_ID_IN_FORMAT = " where id in (%s)";

    private static final String WHERE_PRIORITY_IN_FORMAT = " where priority in (%s) ";

    private static final String UPDATE_CCL_RULE_COUNT_SQL =
        "update " + CONCURRENCY_CONTROL_TRIGGER_TABLE + " set ccl_rule_count=? where priority=?";

    private static final String DELETE_ALL = "delete from " + CONCURRENCY_CONTROL_TRIGGER_TABLE;

    public int insert(CclTriggerRecord record) {
        return insert(INSERT_SQL_FILTER_TRIGGERS, CONCURRENCY_CONTROL_TRIGGER_TABLE, record.buildParams());
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

    public List<CclTriggerRecord> queryByIds(List<String> ids) {
        if (CollectionUtils.isEmpty(ids)) {
            return Collections.EMPTY_LIST;
        }
        String sql = String.format(SELECT_ALL + WHERE_ID_IN_FORMAT + AND_INST_ID + ORDER, concat(ids));
        List<CclTriggerRecord> queryResult = queryBySql(sql);
        return queryResult;
    }

    public List<CclTriggerRecord> query() {
        List<CclTriggerRecord> queryResult = queryBySql(SELECT_ALL + WHERE_INST_ID + ORDER);
        return queryResult;
    }

    public List<CclTriggerRecord> queryByPriorities(List<Integer> priorities) {
        String inContentOfPriorities = StringUtils.join(priorities, ",");
        String sql = String.format(SELECT_ALL + WHERE_PRIORITY_IN_FORMAT + ORDER, inContentOfPriorities);
        List<CclTriggerRecord> queryResult = queryBySql(sql);
        return queryResult;
    }

    private List<CclTriggerRecord> queryBySql(String sql) {
        try {
            return MetaDbUtil.query(sql, CclTriggerRecord.class, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query system table " + CONCURRENCY_CONTROL_TRIGGER_TABLE + " sql: " + sql, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                CONCURRENCY_CONTROL_TRIGGER_TABLE,
                e.getMessage());
        }
    }

    public int updateCclRuleCount(int priority, int cclRuleCount) {
        Map<Integer, ParameterContext> params = new HashMap<>(2);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, cclRuleCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, priority);
        int updatedRows = update(UPDATE_CCL_RULE_COUNT_SQL, GmsSystemTables.CONCURRENCY_CONTROL_TRIGGER, params);
        return updatedRows;
    }

    private int deleteBySql(String sql) {
        try {
            return MetaDbUtil.delete(sql, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to delete system table " + CONCURRENCY_CONTROL_TRIGGER_TABLE + " sql: " + sql, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete",
                CONCURRENCY_CONTROL_TRIGGER_TABLE,
                e.getMessage());
        }
    }

}

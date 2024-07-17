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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author busu
 * date: 2020/10/9 3:15 下午
 */
@ToString
public class CclRuleRecord implements SystemTableRecord {
    public static final int DEFAULT_PARALLELISM = 0;
    public static final int DEFAULT_QUEUE_SIZE = 0;
    public static final int DEFAULT_WAIT_TIMEOUT = 600;
    public static final int DEFAULT_FAST_MATCH = 1;
    public static final int DEFAULT_TRIGGER_PRIORITY = -1;
    public static final int DEFAULT_LIGHT_WAIT = 0;

    public static final int THREAD_WAIT = 0;
    public static final int NO_THREAD_WAIT = 1;

    public String id;
    public String sqlType;
    public String dbName;
    public String tableName;
    public String userName;
    public String clientIp;
    public volatile int parallelism;
    public String keywords;
    public String templateId;
    public String query;
    public String params;
    public String queryTemplateId;
    public volatile int queueSize;
    public long priority;
    public int waitTimeout;
    public int fastMatch;
    public int lightWait;

    public int triggerPriority;

    public Date gmtCreated;
    public Date gmtUpdated;

    @Override
    public CclRuleRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getString("id");
        this.sqlType = rs.getString("sql_type");
        this.dbName = rs.getString("db_name");
        this.tableName = rs.getString("table_name");
        this.userName = rs.getString("user_name");
        this.clientIp = rs.getString("client_ip");
        this.parallelism = rs.getInt("parallelism");
        this.keywords = rs.getString("keywords");
        this.templateId = rs.getString("template_id");
        this.query = rs.getString("query");
        this.params = rs.getString("params");
        this.queryTemplateId = rs.getString("query_template_id");
        this.queueSize = rs.getInt("queue_size");
        this.priority = rs.getInt("priority");
        this.waitTimeout = rs.getInt("wait_timeout");
        this.fastMatch = rs.getInt("fast_match");
        this.lightWait = rs.getInt("light_wait");
        this.triggerPriority = rs.getInt("trigger_priority");
        this.priority = rs.getInt("priority");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtUpdated = rs.getTimestamp("gmt_updated");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(18);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.id);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.sqlType);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.dbName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.tableName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.userName);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.clientIp);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.parallelism);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.keywords);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.templateId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.query);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.params);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.queryTemplateId);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.queueSize);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.waitTimeout);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.fastMatch);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.lightWait);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.triggerPriority);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, InstIdUtil.getInstId());
        return params;
    }

    public CclRuleRecord copy() {
        CclRuleRecord cclRuleRecord = new CclRuleRecord();
        cclRuleRecord.id = this.id;
        cclRuleRecord.sqlType = this.sqlType;
        cclRuleRecord.dbName = this.dbName;
        cclRuleRecord.tableName = this.tableName;
        cclRuleRecord.userName = this.userName;
        cclRuleRecord.clientIp = this.clientIp;
        cclRuleRecord.parallelism = this.parallelism;
        cclRuleRecord.keywords = this.keywords;
        cclRuleRecord.templateId = this.templateId;
        cclRuleRecord.query = this.query;
        cclRuleRecord.params = this.params;
        cclRuleRecord.queryTemplateId = this.queryTemplateId;
        cclRuleRecord.queueSize = this.queueSize;
        cclRuleRecord.priority = this.priority;
        cclRuleRecord.waitTimeout = this.waitTimeout;
        cclRuleRecord.fastMatch = this.fastMatch;
        cclRuleRecord.lightWait = this.lightWait;
        cclRuleRecord.triggerPriority = this.triggerPriority;
        cclRuleRecord.gmtCreated = this.gmtCreated;
        cclRuleRecord.gmtUpdated = this.gmtUpdated;
        return cclRuleRecord;
    }

}

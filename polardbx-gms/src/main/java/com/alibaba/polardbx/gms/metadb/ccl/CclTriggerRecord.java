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
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author busu
 * date: 2021/3/29 10:56 上午
 */
@ToString
public class CclTriggerRecord implements SystemTableRecord {

    public String id;
    public int priority;
    public String conditions;
    public String ruleConfig;
    public String schema;
    public int ruleUpgrade;
    public int maxCclRule;
    public int cclRuleCount;
    public int maxSQLSize;

    public Date gmtCreated;
    public Date gmtUpdated;

    @Override
    public CclTriggerRecord fill(ResultSet rs) throws SQLException {
        this.id = rs.getString("id");
        this.priority = rs.getInt("priority");
        this.conditions = rs.getString("conditions");
        this.ruleConfig = rs.getString("rule_config");
        this.schema = rs.getString("schema");
        this.ruleUpgrade = rs.getInt("rule_upgrade");
        this.maxCclRule = rs.getInt("max_ccl_rule");
        this.cclRuleCount = rs.getInt("ccl_rule_count");
        this.maxSQLSize = rs.getInt("max_sql_size");
        this.gmtCreated = rs.getTimestamp("gmt_created");
        this.gmtUpdated = rs.getTimestamp("gmt_updated");
        return this;
    }

    public Map<Integer, ParameterContext> buildParams() {
        Map<Integer, ParameterContext> params = new HashMap<>(10);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.id);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.priority);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.conditions);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.ruleConfig);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.schema);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.ruleUpgrade);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.maxCclRule);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.cclRuleCount);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setInt, this.maxSQLSize);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, InstIdUtil.getInstId());
        return params;
    }

}

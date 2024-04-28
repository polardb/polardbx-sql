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

package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.lease.LeaseAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pangzhaoxing
 */
public class EncdbRuleAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncdbRuleAccessor.class);

    public static final String ENCDB_RULE = wrap(GmsSystemTables.ENCDB_RULE);

    private static final String COLUMNS = "`name`, `enable`, `meta`,`users`,`description`";

    private static final String VALUES = "?,?,?,?,?";

    private static final String REPLACE_RULE =
        "replace into " + ENCDB_RULE + "(" + COLUMNS + ") values (" + VALUES + ")";

    private static final String INSERT_RULE = "insert into " + ENCDB_RULE + "(" + COLUMNS + ") values (" + VALUES + ")";

    private static final String SELECT_ALL_ENABLED_RULE =
        "select " + COLUMNS + " from " + ENCDB_RULE + " where enable=1";

    private static final String DELETE_RULE_BY_NAME = "delete from " + ENCDB_RULE + " where name=?";

    public int replaceRule(EncdbRule rule) {
        return insert(REPLACE_RULE, ENCDB_RULE, rule.buildInsertParams());
    }

    public int insertRule(EncdbRule rule) {
        return insert(INSERT_RULE, ENCDB_RULE, rule.buildInsertParams());
    }

    public List<EncdbRule> queryAllEnabledRules() {
        return query(SELECT_ALL_ENABLED_RULE, ENCDB_RULE, EncdbRule.class, (Map<Integer, ParameterContext>) null);
    }

    public int deleteRuleByName(String ruleName) {
        Map<Integer, ParameterContext> params = new HashMap<>(1);
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, ruleName);
        return delete(DELETE_RULE_BY_NAME, ENCDB_RULE, params);
    }

}

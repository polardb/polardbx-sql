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

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.privilege.PolarAccount;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class EncdbRuleMatchTree {

    private static final Logger LOG = LoggerFactory.getLogger(EncdbRuleMatchTree.class);

    private Map<String, EncdbRule> rules;

    //db -> table -> col -> rule_names
    private Map<String, Map<String, Map<String, String>>> tree;

    public EncdbRuleMatchTree() {
        rules = new HashMap<>();
        tree = new HashMap<>();
    }

    /**
     * @param tableSet a set of <schema, table> pair, the schema may be null
     * @param schema schema of the table whose schema is null in tableSet
     * @return exist any rules match any tables in tableSet
     */
    public boolean fastMatch(Set<Pair<String, String>> tableSet, String schema) {
        for (Pair<String, String> table : tableSet) {
            if (fastMatch(table.getKey() == null ? schema : table.getKey(), table.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param originColumnNamesList origin column names derived from sql resultSet columns
     * @return whether the output columns need encrypted
     */
    public boolean[] getColumnEncBitmap(List<List<String[]>> originColumnNamesList, PolarAccount account) {
        boolean[] columnEncBitmap = new boolean[originColumnNamesList.size()];
        boolean hasEncColumn = false;
        for (int i = 0; i < originColumnNamesList.size(); i++) {
            if (getColumnEncBit(originColumnNamesList.get(i), account)) {
                columnEncBitmap[i] = true;
                hasEncColumn = true;
            }
        }
        return hasEncColumn ? columnEncBitmap : null;
    }

    public boolean getColumnEncBit(List<String[]> originColumnNames, PolarAccount account) {
        for (String[] col : originColumnNames) {
            String matchRuleName = match(col[0], col[1], col[2]);
            if (matchRuleName == null) {
                continue;
            }
            EncdbRule rule = getRule(matchRuleName);
            if (rule != null && rule.userMatch(account)) {
                return true;
            }
        }
        return false;
    }

    public List<Set<String>> getColumnMatchRulesList(List<List<String[]>> originColumnNamesList) {
        List<Set<String>> matchRulesList = new ArrayList<>(originColumnNamesList.size());
        boolean hasMatchRule = false;
        for (int i = 0; i < originColumnNamesList.size(); i++) {
            matchRulesList.add(getColumnMatchRules(originColumnNamesList.get(i)));
            if (matchRulesList.get(i).size() > 0) {
                hasMatchRule = true;
            }
        }
        return hasMatchRule ? matchRulesList : null;
    }

    public Set<String> getColumnMatchRules(List<String[]> originColumnNames) {
        Set<String> matchRules = new HashSet<>();
        for (String[] col : originColumnNames) {
            String matchRule = match(col[0], col[1], col[2]);
            if (matchRule != null) {
                matchRules.add(matchRule);
            }
        }
        return matchRules;
    }

    public boolean fastMatch(String db, String tb) {
        db = db.toLowerCase();
        tb = tb.toLowerCase();
        if (tree.containsKey(db)) {
            if (tree.get(db).containsKey(tb)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取只包含db、tb的加密规则
     */
    public Set<String> getSpecificTableRules(String db, String tb) {
        db = db.toLowerCase();
        tb = tb.toLowerCase();
        if (tree.containsKey(db)) {
            if (tree.get(db).containsKey(tb)) {
                Collection<String> matchRules = tree.get(db).get(tb).values();
                Set<String> specificTableRules = matchRules.stream()
                    .filter(ruleName -> {
                        EncdbRule rule = rules.get(ruleName);
                        return rule.getDbs().size() == 1 && rule.getTbs().size() == 1;
                    }).collect(Collectors.toSet());
                return specificTableRules;
            }
        }
        return Collections.emptySet();
    }

    public String match(String db, String tb, String col) {
        db = db.toLowerCase();
        tb = tb.toLowerCase();
        col = col.toLowerCase();
        if (tree.containsKey(db)) {
            if (tree.get(db).containsKey(tb)) {
                return tree.get(db).get(tb).get(col);
            }
        }
        return null;
    }

    public void insertRule(EncdbRule rule) {
        rules.put(rule.getRuleName(), rule);
        for (String db : rule.getDbs()) {
            tree.putIfAbsent(db, new HashMap<>());
            Map<String, Map<String, String>> dbSubTree = tree.get(db);
            for (String tb : rule.getTbs()) {
                dbSubTree.putIfAbsent(tb, new HashMap<>());
                Map<String, String> tbSubTree = dbSubTree.get(tb);
                for (String col : rule.getCols()) {
                    if (tbSubTree.containsKey(col)) {
                        LOG.warn("column should only exist in one encdb rule : "
                            + col + " in " + tbSubTree.get(col) + " 、 " + rule.getName());
                    }
                    tbSubTree.put(col, rule.getRuleName());
                }
            }
        }
    }

    public EncdbRule getRule(String ruleName) {
        return rules.get(ruleName);
    }

}

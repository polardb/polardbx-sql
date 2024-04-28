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

package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.PolarPrivileges;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.common.encdb.enums.MsgType;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRuleManager;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbRule;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants.*;
import static com.alibaba.polardbx.gms.metadb.encdb.EncdbRule.*;

/**
 * @author pangzhaoxing
 */
public class EncdbImportRuleHandler implements EncdbHandler {

    private static final int RULE_VERSION = 1;

    @Override
    public JSONObject handle(JSONObject request, ServerConnection serverConnection) {
        EncdbMsgProcessor.checkUserPrivileges(serverConnection, true);

        List<EncdbRule> encdbRules = json2Rule(request);
        EncdbRuleManager.getInstance().insertEncRules(encdbRules);

        return EMPTY;
    }

    public static List<EncdbRule> json2Rule(JSONObject request) {
        JSONObject encRule =
            JSON.parseObject(new String(Utils.base64ToBytes(request.getString(ENC_RULE)), StandardCharsets.UTF_8));

        int version = encRule.getIntValue(VERSION);
        if (version != RULE_VERSION) {
            throw new EncdbException("the rule format is invalid");
        }
        JSONArray rules = encRule.getJSONArray(RULES);
        List<EncdbRule> encdbRules = new ArrayList<>(rules.size());
        for (int i = 0; i < rules.size(); i++) {
            JSONObject ruleJson = rules.getJSONObject(i);
            String ruleName = ruleJson.getString(NAME);
            boolean enable = ruleJson.getBoolean(ENABLED);
            String description = ruleJson.getString(DESCRIPTION);

            JSONObject meta = ruleJson.getJSONObject(META);
            Set<String> dbs = jsonArr2Set(meta.getJSONArray(DATABASES));
            Set<String> tbs = jsonArr2Set(meta.getJSONArray(TABLES));
            Set<String> cols = jsonArr2Set(meta.getJSONArray(COLUMNS));

            JSONObject users = ruleJson.getJSONObject(USERS);
            Set<PolarAccount> fullAccessUsers = users.getJSONArray(FULL_ACCESS)
                .stream().map(s -> PolarAccount.fromIdentifier((String) s))
                .collect(Collectors.toSet());
            Set<PolarAccount> restrictedAccessUsers = users.getJSONArray(RESTRICTED_ACCESS)
                .stream().map(s -> PolarAccount.fromIdentifier((String) s))
                .collect(Collectors.toSet());

            encdbRules.add(
                new EncdbRule(ruleName, enable, fullAccessUsers, restrictedAccessUsers, dbs, tbs, cols, description)
            );
        }

        return encdbRules;
    }

    public static JSONObject rule2Json(List<EncdbRule> encdbRules) {
        JSONObject json = new JSONObject();
        json.put(REQUEST_TYPE, MsgType.ENC_RULE_IMPORT);

        JSONObject encRule = new JSONObject();
        encRule.put(VERSION, RULE_VERSION);
        JSONArray rules = new JSONArray();
        encRule.put(RULES, rules);
        for (EncdbRule encdbRule : encdbRules) {
            JSONObject ruleJson = new JSONObject();
            ruleJson.put(NAME, encdbRule.getRuleName());
            ruleJson.put(ENABLED, encdbRule.isEnable());
            ruleJson.put(DESCRIPTION, encdbRule.getDescription());

            JSONObject ruleMeta = new JSONObject();
            ruleJson.put(META, ruleMeta);
            ruleMeta.put(DATABASES, set2JsonArr(encdbRule.getDbs()));
            ruleMeta.put(TABLES, set2JsonArr(encdbRule.getTbs()));
            ruleMeta.put(COLUMNS, set2JsonArr(encdbRule.getCols()));

            JSONObject users = new JSONObject();
            ruleJson.put(USERS, users);
            users.put(FULL_ACCESS,
                new JSONArray(encdbRule.getFullAccessUsers().stream().map(PolarAccount::getIdentifier)
                    .collect(Collectors.toList())));
            users.put(RESTRICTED_ACCESS,
                new JSONArray(encdbRule.getRestrictedAccessUsers().stream().map(PolarAccount::getIdentifier)
                    .collect(Collectors.toList())));
            rules.add(ruleJson);
        }

        json.put(ENC_RULE, Utils.bytesTobase64(encRule.toString().getBytes(StandardCharsets.UTF_8)));
        return json;
    }

}

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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class EncdbRule implements SystemTableRecord {

    private static final String DATABASES = "databases";

    private static final String TABLES = "tables";

    private static final String COLUMNS = "columns";

    private static final String FULL_ACCESS = "fullAccess";

    private static final String RESTRICTED_ACCESS = "restrictedAccess";

    private String name;

    private boolean enable;

    private Set<PolarAccount> fullAccessUsers;

    private Set<PolarAccount> restrictedAccessUsers;

    private Set<String> dbs;

    private Set<String> tbs;

    private Set<String> cols;

    private String description;

    public EncdbRule() {
    }

    public EncdbRule(String name, boolean enable, Set<PolarAccount> fullAccessUsers,
                     Set<PolarAccount> restrictedAccessUsers, Set<String> dbs, Set<String> tbs, Set<String> cols,
                     String description) {
        this.name = name;
        this.enable = enable;
        this.fullAccessUsers = fullAccessUsers;
        this.restrictedAccessUsers = restrictedAccessUsers;
        this.dbs = dbs.stream().map(String::toLowerCase).collect(Collectors.toSet());
        this.tbs = tbs.stream().map(String::toLowerCase).collect(Collectors.toSet());
        this.cols = cols.stream().map(String::toLowerCase).collect(Collectors.toSet());
        this.description = description;
    }

    public boolean userMatch(PolarAccount account) {
        for (PolarAccount fullAccessUser : fullAccessUsers) {
            if (fullAccessUser.matches(account.getUsername(), account.getHost())) {
                return false;
            }
        }
        //除了full access users，均认为要加密。
        return true;
    }

    @Override
    public EncdbRule fill(ResultSet rs) throws SQLException {
        this.name = rs.getString("name");
        this.enable = rs.getBoolean("enable");
        this.description = rs.getString("description");

        JSONObject meta = JSON.parseObject(rs.getString("meta"));
        this.dbs = jsonArr2Set(meta.getJSONArray(DATABASES));
        this.tbs = jsonArr2Set(meta.getJSONArray(TABLES));
        this.cols = jsonArr2Set(meta.getJSONArray(COLUMNS));

        JSONObject users = JSON.parseObject(rs.getString("users"));
        this.fullAccessUsers = users.getJSONArray(FULL_ACCESS)
            .stream().map(s -> PolarAccount.fromIdentifier((String) s))
            .collect(Collectors.toSet());
        this.restrictedAccessUsers = users.getJSONArray(RESTRICTED_ACCESS)
            .stream().map(s -> PolarAccount.fromIdentifier((String) s))
            .collect(Collectors.toSet());
        return this;
    }

    /**
     * 必须和fill(ResultSet rs)对应
     */
    protected Map<Integer, ParameterContext> buildInsertParams() {
        JSONObject meta = new JSONObject();
        meta.put(DATABASES, set2JsonArr(dbs));
        meta.put(TABLES, set2JsonArr(tbs));
        meta.put(COLUMNS, set2JsonArr(cols));

        JSONObject users = new JSONObject();
        users.put(RESTRICTED_ACCESS,
            new JSONArray(
                restrictedAccessUsers.stream().map(PolarAccount::getIdentifier).collect(Collectors.toList())));
        users.put(FULL_ACCESS,
            new JSONArray(fullAccessUsers.stream().map(PolarAccount::getIdentifier).collect(Collectors.toList())));

        Map<Integer, ParameterContext> params = new HashMap<>(5);
        int index = 0;
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.name);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setBoolean, this.enable);
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, meta.toString());
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, users.toString());
        MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, this.description);

        return params;
    }

    public static JSONArray set2JsonArr(Set<String> set) {
        JSONArray array = new JSONArray(set.size());
        array.addAll(set);
        return array;
    }

    public static Set<String> jsonArr2Set(JSONArray jsonArray) {
        Set<String> set = new HashSet<>();
        for (int i = 0; i < jsonArray.size(); i++) {
            set.add(jsonArray.getString(i).toLowerCase());
        }
        return set;
    }

    public String getRuleName() {
        return name;
    }

    public boolean isEnable() {
        return enable;
    }

    public String getDescription() {
        return description;
    }

    public Set<String> getDbs() {
        return dbs;
    }

    public Set<String> getTbs() {
        return tbs;
    }

    public Set<String> getCols() {
        return cols;
    }

    public String getName() {
        return name;
    }

    public Set<PolarAccount> getFullAccessUsers() {
        return fullAccessUsers;
    }

    public Set<PolarAccount> getRestrictedAccessUsers() {
        return restrictedAccessUsers;
    }

    @Override
    public EncdbRule clone() {
        return new EncdbRule(name, enable,
            fullAccessUsers.stream().map(PolarAccount::deepCopy).collect(Collectors.toSet()),
            restrictedAccessUsers.stream().map(PolarAccount::deepCopy).collect(Collectors.toSet()),
            new HashSet<>(dbs), new HashSet<>(tbs), new HashSet<>(cols), description);
    }
}

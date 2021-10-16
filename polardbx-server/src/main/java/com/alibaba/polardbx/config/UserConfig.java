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

package com.alibaba.polardbx.config;

import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.taobao.tddl.common.privilege.EncrptPassword;
import com.taobao.tddl.common.privilege.GrantParameter;
import com.taobao.tddl.common.privilege.GrantedUser;
import com.alibaba.polardbx.common.privilege.Host;
import com.taobao.tddl.common.privilege.PrivilegeLevel;
import com.alibaba.polardbx.common.privilege.PrivilegeUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 用户账户信息
 *
 * @author xianmao.hexm 2011-1-11 下午02:26:09
 */
public class UserConfig {

    private String name;
    private Host host;
    private EncrptPassword encrptPassword;
    private Set<String> schemas;         // 对应可访问的schema信息
    private Map<String, DbPriv> schemaPrivs;     // 库级权限
    private Map<String/* dbName */, Map<String/* tbName */, TbPriv>> schemaTbPrivs;   // 表级权限

    private String clusterPasswd;
    private String globalPasswd;
    private String clusterEncPasswd;
    private String globalEncPasswd;

    public static UserConfig createUserWithoutPrivs(String schema, GrantedUser user) {
        UserConfig uc = new UserConfig();
        uc.setName(user.getUser());
        uc.setHost(new Host(user.getHost()));
        uc.setEncrptPassword(user.getEncrptPassword());
        uc.setSchemas(Sets.newHashSet(schema));
        return uc;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Host getHost() {
        return host;
    }

    public void setHost(Host host) {
        this.host = host;
    }

    public EncrptPassword getEncrptPassword() {
        return encrptPassword;
    }

    public void setEncrptPassword(EncrptPassword encrptPassword) {
        this.encrptPassword = encrptPassword;
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public Map<String, DbPriv> getSchemaPrivs() {
        return schemaPrivs;
    }

    public void setSchemaPrivs(Map<String, DbPriv> schemaPrivs) {
        this.schemaPrivs = schemaPrivs;
    }

    public Map<String/* dbName */, Map<String/* tbName */, TbPriv>> getSchemaTbPrivs() {
        return schemaTbPrivs;
    }

    public void setSchemaTbPrivs(Map<String/* dbName */, Map<String/* tbName */, TbPriv>> schemaTbPrivs) {
        this.schemaTbPrivs = schemaTbPrivs;
    }

    /**
     * 根据授权参数重新构建新的权限信息
     */
    public void buildNewSchemaInfos(GrantParameter grantParameter) {
        this.schemas = Sets.newHashSet(grantParameter.getDatabase());
        String database = TStringUtil.normalizePriv(grantParameter.getDatabase());
        this.schemaPrivs = null;
        this.schemaTbPrivs = null;

        if (grantParameter.getPrivilegeLevel() == PrivilegeLevel.DATABASE) {
            DbPriv dbPriv = new DbPriv(database);
            PrivilegeUtil.addPrivilegePointsToDbPriv(dbPriv, grantParameter);

            this.schemaPrivs = Maps.newHashMap();
            this.schemaPrivs.put(database, dbPriv);
        } else if (grantParameter.getPrivilegeLevel() == PrivilegeLevel.TABLE) {
            TbPriv tbPriv = new TbPriv(database, grantParameter.getTable());
            PrivilegeUtil.addPrivilegePointsToTbPriv(tbPriv, grantParameter);

            this.schemaTbPrivs = Maps.newHashMap();
            Map<String, TbPriv> tbPrivMap = Maps.newHashMap();
            this.schemaTbPrivs.put(database, tbPrivMap);
            tbPrivMap.put(grantParameter.getTable(), tbPriv);
        }
    }

    public String getClusterPasswd() {
        return clusterPasswd;
    }

    public void setClusterPasswd(String clusterPasswd) {
        this.clusterPasswd = clusterPasswd;
    }

    public String getGlobalPasswd() {
        return globalPasswd;
    }

    public void setGlobalPasswd(String globalPasswd) {
        this.globalPasswd = globalPasswd;
    }

    public String getClusterEncPasswd() {
        return clusterEncPasswd;
    }

    public void setClusterEncPasswd(String clusterEncPasswd) {
        this.clusterEncPasswd = clusterEncPasswd;
    }

    public String getGlobalEncPasswd() {
        return globalEncPasswd;
    }

    public void setGlobalEncPasswd(String globalEncPasswd) {
        this.globalEncPasswd = globalEncPasswd;
    }
}

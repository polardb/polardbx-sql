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

package com.alibaba.polardbx;

import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.common.CommonUtils;
import com.alibaba.polardbx.config.IUserHostDefination;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.config.UserConfig;
import com.alibaba.polardbx.net.handler.Privileges;
import com.taobao.tddl.common.privilege.EncrptPassword;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author xianmao.hexm
 */
public class CobarPrivileges implements Privileges {

    @Override
    public boolean schemaExists(String schema) {
        if (ConfigDataMode.isFastMock()) {
            return true;
        }
        CobarConfig conf = CobarServer.getInstance().getConfig();
        Map<String, SchemaConfig> schemas = conf.getSchemas();
        return schemas == null ? false : schemas.containsKey(schema);
    }

    @Override
    public boolean checkQuarantine(String user, String host) {
        if (ConfigDataMode.isFastMock()) {
            return true;
        }
        /**
         * Always check cluster level blacklist firstly.
         */
        CobarConfig conf = CobarServer.getInstance().getConfig();

        // 系统IP检查
        if (conf.getClusterQuarantine() != null) {
            // 全局黑名单
            if (conf.getClusterQuarantine().getClusterBlacklist() != null
                && conf.getClusterQuarantine().getClusterBlacklist().containsOf(host)) {
                return false;
            }

            // 全局白名单
            if (conf.getClusterQuarantine().getClusterWhitelist() != null
                && conf.getClusterQuarantine().getClusterWhitelist().containsOf(host)) {
                return true;
            }

            // cluster node ips should use instanceId
            if (conf.getClusterQuarantine().getClusterIps() != null
                && conf.getClusterQuarantine().getClusterIps().containsOf(host)) {
                if (CobarServer.getInstance().getConfig().getSystem().getInstanceId().equals(user)) {
                    return true;
                }
            }
        }

        /**
         * No cluster level black list then check app level whitelist.
         */
        Map<String, IUserHostDefination> quarantineHosts = conf.getQuarantine().getHosts();
        /* Unlike original pattern, use appName/user as key to do search */
        Set<String> schemas;
        if (isPrivilegeMode()) {
            schemas = getUserSchemas(user, host);
        } else {
            schemas = getUserSchemas(user);
        }

        // if blocked by all schema's quarantines
        boolean allQuarantinesBlocked = true;

        if (CommonUtils.isEmpty(schemas)) {
            allQuarantinesBlocked = false;
        } else {
            for (String schema : schemas) {
                if (quarantineHosts != null && quarantineHosts.containsKey(schema)) {
                    IUserHostDefination hosts = quarantineHosts.get(schema);
                    if (!hosts.isEmpty()) {
                        allQuarantinesBlocked &= !hosts.containsOf(host);
                        continue;
                    }
                }
                allQuarantinesBlocked = false;
            }
        }

        return !allQuarantinesBlocked;
    }

    @Override
    public boolean userExists(String user) {
        if (ConfigDataMode.isFastMock()) {
            return true;
        }
        CobarConfig conf = CobarServer.getInstance().getConfig();
        if (isPrivilegeMode()) {
            return conf.getAuthorizeConfig().userExists(user);
        } else {
            Map<String, UserConfig> users = conf.getUsers();
            return users == null ? false : users.containsKey(user);
        }
    }

    @Override
    public boolean userExists(String user, String host) {
        if (ConfigDataMode.isFastMock()) {
            return true;
        }
        return getAuthorizeConfig().userExists(user, host);
    }

    @Override
    public boolean userMatches(String user, String host) {
        return getMatchedUserConfig(user, host) != null;
    }

    @Override
    public EncrptPassword getPassword(String user) {
        CobarConfig conf = CobarServer.getInstance().getConfig();
        Map<String, UserConfig> users = conf.getUsers();
        if (users == null) {
            return null;
        }

        UserConfig uc = users.get(user);
        if (uc != null) {
            return uc.getEncrptPassword();
        } else {
            return null;
        }
    }

    @Override
    public EncrptPassword getPassword(String user, String host) {
        UserConfig uc = getMatchedUserConfig(user, host);
        if (uc != null) {
            return uc.getEncrptPassword();
        } else {
            return null;
        }
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        CobarConfig conf = CobarServer.getInstance().getConfig();
        Map<String, UserConfig> users = conf.getUsers();
        if (users == null) {
            return null;
        }

        UserConfig uc = users.get(user);
        if (uc != null) {
            return uc.getSchemas();
        } else {
            return null;
        }
    }

    @Override
    public Set<String> getUserSchemas(String user, String host) {
        UserConfig uc = getAuthorizeConfig().getMatchedUser(user, host);
        return uc != null ? uc.getSchemas() : null;
    }

    @Override
    public boolean isTrustedIp(String host, String user) {
        CobarConfig conf = CobarServer.getInstance().getConfig();
        if (conf.getClusterQuarantine() != null && conf.getClusterQuarantine().getTrustedIps() != null) {
            if (conf.getClusterQuarantine().getTrustedIps().containsOf(host)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Map<String, DbPriv> getSchemaPrivs(String user, String host) {
        UserConfig uc = getMatchedUserConfig(user, host);
        if (uc == null) {
            return Collections.emptyMap();
        }

        Map<String, DbPriv> privileges = uc.getSchemaPrivs();
        if (privileges == null) {
            return Collections.emptyMap();
        }

        return privileges;
    }

    @Override
    public Map<String, TbPriv> getTablePrivs(String user, String host, String database) {
        UserConfig uc = getMatchedUserConfig(user, host);
        if (uc == null) {
            return Collections.emptyMap();
        }

        Map<String, Map<String, TbPriv>> schemaTbPrivs = uc.getSchemaTbPrivs();
        if (schemaTbPrivs == null) {
            return Collections.emptyMap();
        }

        Map<String, TbPriv> privileges = schemaTbPrivs.get(database);
        if (privileges == null) {
            return Collections.emptyMap();
        }
        return privileges;
    }

    private AuthorizeConfig getAuthorizeConfig() {
        return CobarServer.getInstance().getConfig().getAuthorizeConfig();
    }

    private UserConfig getMatchedUserConfig(String user, String host) {
        return getAuthorizeConfig().getMatchedUser(user, host);
    }

    private boolean isPrivilegeMode() {
        return true;
    }
}

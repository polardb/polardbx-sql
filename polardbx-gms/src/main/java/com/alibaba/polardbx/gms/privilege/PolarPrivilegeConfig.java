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

package com.alibaba.polardbx.gms.privilege;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.Config;
import com.alibaba.polardbx.config.Configurable;
import org.apache.commons.lang.StringUtils;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Privilege related configurations.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables .html#sysvar_mandatory_roles">Mandatory
 * Roles</a>
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_activate_all_roles_on_login">roles</a>
 * @since 5.4.9
 */
public class PolarPrivilegeConfig implements Configurable {
    private static final Logger LOGGER = LoggerFactory.getLogger(PolarPrivilegeConfig.class);
    private static final String CONFIG_PREFIX = "POLARX_PRIV_";
    static final String CONFIG_MANDATORY_ROLES = CONFIG_PREFIX + "MANDATORY_ROLES";
    static final String CONFIG_ACTIVE_ALL_ROLES_ON_LOGIN = CONFIG_PREFIX + "ACTIVE_ALL_ROLES_ON_LOGIN";
    static final String CONFIG_ENABLE_RIGHTS_SEPARATION = CONFIG_PREFIX + "ENABLE_RIGHTS_SEPARATION";

    private static final Map<String, MethodHandle> CONFIG_HANDLERS =
        Configurable.parseConfigurableProps(PolarPrivilegeConfig.class);

    private final PolarPrivManager manager;

    @Config(key = CONFIG_MANDATORY_ROLES, configMethod = "updateMandatoryRoles")
    private volatile String mandatoryRoles = "";
    @Config(key = CONFIG_ACTIVE_ALL_ROLES_ON_LOGIN, configMethod = "updateActiveAllRolesOnLogin")
    private volatile boolean activeAllRolesOnLogin = false;

    @Config(key = CONFIG_ENABLE_RIGHTS_SEPARATION, configMethod = "updateRightsSeparationMode")
    private volatile boolean rightsSeparationEnabled = false;

    private volatile List<Long> mandatoryRoleIds = Collections.emptyList();

    public PolarPrivilegeConfig(PolarPrivManager manager) {
        this.manager = manager;
    }

    public void updateMandatoryRoles(String mandatoryRolesValue) {
        try {
            Set<Long> newMandatoryRoleIds = parseAndGetMandatoryRoleIds(mandatoryRolesValue);
            this.mandatoryRoles = mandatoryRolesValue;
            this.mandatoryRoleIds = new ArrayList<>(newMandatoryRoleIds);
        } catch (Exception e) {
            LOGGER.error("Failed to update mandatory values: " + mandatoryRolesValue, e);
        }
    }

    private Set<Long> parseAndGetMandatoryRoleIds(String mandatoryRolesValue) {
        if (StringUtils.isBlank(mandatoryRolesValue)) {
            return Collections.emptySet();
        }

        Set<Long> newMandatoryRoleIds = new TreeSet<>();
        for (String mandatoryRole : mandatoryRolesValue.split(",")) {
            PolarAccountInfo accountInfo = manager.getExactUserByIdentifier(mandatoryRole.trim());
            if (accountInfo == null) {
                LOGGER.error("Role " + mandatoryRole + " does not exist, ignoring.");
                continue;
            }
            if (accountInfo.getAccountType() != AccountType.ROLE) {
                LOGGER.error(mandatoryRole + " is not role, ignoring.");
                continue;
            }
            newMandatoryRoleIds.add(accountInfo.getAccountId());
        }

        return newMandatoryRoleIds;
    }

    public void updateActiveAllRolesOnLogin(String value) {
        setActiveAllRolesOnLogin(Boolean.parseBoolean(value));
    }

    public String getMandatoryRoles() {
        return mandatoryRoles;
    }

    public void setMandatoryRoles(String mandatoryRoles) {
        this.mandatoryRoles = mandatoryRoles;
    }

    public boolean isActiveAllRolesOnLogin() {
        return activeAllRolesOnLogin;
    }

    public void setActiveAllRolesOnLogin(boolean activeAllRolesOnLogin) {
        this.activeAllRolesOnLogin = activeAllRolesOnLogin;
    }

    public List<Long> getMandatoryRoleIds() {
        return mandatoryRoleIds;
    }

    public void updateRightsSeparationMode(String value) {
        try {
            this.rightsSeparationEnabled = Boolean.parseBoolean(value);
            // We don't check value change here since sometimes we want to force update of rights separation mode
            manager.changeRightsSeparationMode(this.rightsSeparationEnabled);
        } catch (Exception e) {
            LOGGER.error("Failed to enable right separation mode!", e);
        }
    }

    public boolean isRightsSeparationEnabled() {
        return rightsSeparationEnabled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, MethodHandle> getConfigHandlers() {
        return CONFIG_HANDLERS;
    }
}

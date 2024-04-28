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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.ActiveRoles;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.server.ServerConnection;
import com.taobao.tddl.common.privilege.EncrptPassword;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.alibaba.polardbx.gms.privilege.PolarPrivUtil.POLAR_ROOT;

/**
 * @author shicai.xsc 2020/3/5 02:02
 * @since 5.0.0.0
 */
public class PolarPrivileges extends CobarPrivileges implements Privileges {

    private FrontendConnection userConnection;

    private PolarPrivManager privManager = PolarPrivManager.getInstance();

    private PolarQuarantineManager quantineManager = PolarQuarantineManager.getInstance();

    public PolarPrivileges(FrontendConnection connection) {
        userConnection = connection;
    }

    /*------------------------------------------privilege manage------------------------------------------------------*/

    // region privilege manage
    @Override
    public boolean userMatches(String user, String host) {
        return getMatchUser(user, host) != null;
    }

    @Override
    public EncrptPassword getPassword(String user, String host) {
        PolarAccountInfo matchUser = getMatchUser(user, host);
        if (matchUser == null) {
            throw GeneralUtil.nestedException(String.format("user '%s'@'%s' does not exist", user, host));
        }
        return new EncrptPassword(matchUser.getPassword(), matchUser.getAuthPlugin(), true);
    }

    @Override
    public Set<String> getUserSchemas(String user, String host) {
        PolarAccountInfo userInfo = getMatchUser(user, host);
        Set<String> res = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if (userInfo == null) {
            throw GeneralUtil.nestedException(String.format("user '%s'@'%s' does not exist", user, host));
        }

        ActiveRoles activeRoles = ActiveRoles.noneValue();
        if (userConnection instanceof ServerConnection) {
            activeRoles = ((ServerConnection) userConnection).getActiveRoles();
        }
        Set<String> schemas = privManager.getUsageDbs(userInfo.getUsername(), userInfo.getHost(), activeRoles);
        if (schemas != null) {
            res.addAll(schemas);
        }

        // always have privilege on polardbx
        res.add(SystemDbHelper.DEFAULT_DB_NAME);
        // always have privilege on cdc db when user is polar root
        if (StringUtils.equals(POLAR_ROOT, user)) {
            res.add(SystemDbHelper.CDC_DB_NAME);
        }

        return res;
    }

    @Override
    public boolean userExists(String user, String host) {
        return userMatches(user, host);
    }

    @Override
    public boolean schemaExists(String schema) {
        return privManager.getAllDbs().contains(schema);
    }

    public PolarAccountInfo checkAndGetMatchUser(String user, String host) {
        PolarAccountInfo userInfo = getMatchUser(user, host);
        if (userInfo == null) {
            throw GeneralUtil.nestedException(String.format("user '%s'@'%s' does not exist", user, host));
        }
        return userInfo;
    }

    private PolarAccountInfo getMatchUser(String user, String host) {
        // case :
        // 1. there are 2 users exist: p1@'127.0.0.1' and p1@'%'
        // 2. user login and matches p1@'127.0.0.1'
        // 3. p1@'127.0.0.1' dropped by admin
        // 4. user should not match p1@'%', instead he should be warned
        // "user 'p1'@'127.0.0.1' does not exist"
        if (userConnection.getMatchPolarUserInfo() != null) {
            user = userConnection.getMatchPolarUserInfo().getUsername();
            host = userConnection.getMatchPolarUserInfo().getHost();
        }

        PolarAccountInfo userInfo =
            userConnection.getMatchPolarUserInfo() != null ? privManager.getExactUser(user, host) :
                privManager.getMatchUser(user,
                    host);

        // Role can't be used for log in
        if (userInfo != null && userInfo.getAccountType() == AccountType.ROLE) {
            return null;
        }
        return userInfo;
    }

    // endregion

    /*------------------------------------------quantine manage-------------------------------------------------------*/

    // region quantine manage
    @Override
    public boolean checkQuarantine(String user, String host) {
        return quantineManager.checkQuarantine(host);
    }

    @Override
    public boolean isTrustedIp(String host, String user) {
        return quantineManager.isTrustedIp(host);
    }

    // endregion

    /*------------------------------------------non-implemented functions---------------------------------------------*/

    // region non-implementd functions
    @Override
    public boolean userExists(String user) {
        throw new NotImplementedException();
    }

    @Override
    public EncrptPassword getPassword(String user) {
        throw new NotImplementedException();
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, DbPriv> getSchemaPrivs(String user, String host) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, TbPriv> getTablePrivs(String user, String host, String database) {
        throw new NotImplementedException();
    }
    // endregion
}

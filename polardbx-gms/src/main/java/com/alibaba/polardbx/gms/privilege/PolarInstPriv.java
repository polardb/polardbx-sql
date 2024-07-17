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

import com.taobao.tddl.common.privilege.AuthPlugin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author shicai.xsc 2020/3/3 13:34
 * @since 5.0.0.0
 */
public class PolarInstPriv extends BasePolarPriv {
    public PolarInstPriv() {
        super(PrivilegeScope.INSTANCE);
    }

//    private boolean createUserPriv;
//    private boolean metaDbPriv;

    static Collection<PolarAccountInfo> loadInstPrivs(Connection conn, Collection<PolarAccount> accounts)
        throws SQLException {
        final String sql = String.format("SELECT * FROM %s WHERE %s = ? and %s = ?", PolarPrivUtil.USER_PRIV_TABLE,
            PolarPrivUtil.USER_NAME, PolarPrivUtil.HOST);
        List<PolarAccountInfo> accountInfos = new ArrayList<>(accounts.size());
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (PolarAccount account : accounts) {
                stmt.setString(1, account.getUsername());
                stmt.setString(2, account.getHost());

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        accountInfos.add(loadInstPriv(rs));
                    }
                }
            }
        }

        return accountInfos;
    }

    static PolarAccountInfo loadInstPriv(ResultSet rs) throws SQLException {
        PolarInstPriv instPriv = new PolarInstPriv();
        loadBasePriv(rs, instPriv);
        Long accountId = rs.getLong(PolarPrivUtil.ACCOUNT_ID);

        String password = rs.getString(PolarPrivUtil.PASSWORD);
        String plugin = rs.getString(PolarPrivUtil.PLUGIN);
        PolarAccountInfo userInfo = new PolarAccountInfo(PolarAccount.newBuilder()
            .setAccountId(accountId)
            .setAccountType(AccountType.lookupByCode(rs.getByte(PolarPrivUtil.ACCOUNT_TYPE)))
            .setUsername(instPriv.getUserName())
            .setHost(instPriv.getHost())
            .setPassword(password)
            .setAuthPlugin(AuthPlugin.lookupByName(plugin))
            .build());

        userInfo.setInstPriv(instPriv);
        return userInfo;
    }

    @Override
    public String getIdentifier() {
        return userName + "@" + host;
    }

    @Override
    public String toInsertNewSql() {
        throw new UnsupportedOperationException("toInsertNewSql");
    }

    public PolarInstPriv deepCopy() {
        PolarInstPriv clone = new PolarInstPriv();
        copy(this, clone);
//        clone.createUserPriv = this.createUserPriv;
//        clone.metaDbPriv = this.metaDbPriv;
        return clone;
    }

    public Optional<String> showGrantsResult(PolarAccount user) {
        return super.showGrantsResult(user, "*.*", true);
    }

    @Override
    public boolean equals(final Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof PolarInstPriv)) {
            return false;
        }
        final PolarInstPriv other = (PolarInstPriv) o;
        if (!other.canEqual((Object) this)) {
            return false;
        }
        return true;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof PolarInstPriv;
    }

    @Override
    public int hashCode() {
        int result = 1;
        return result;
    }

    @Override
    public String toString() {
        return "PolarInstPriv()";
    }

    public Optional<String> toUpdatePrivilegeSql(boolean grant) {
        return toSetPrivilegeSql(grant)
            .map(setSql -> String.format("update %s set %s where %s = '%s' and %s = '%s'",
                PolarPrivUtil.USER_PRIV_TABLE, setSql, PolarPrivUtil.USER_NAME, userName, PolarPrivUtil.HOST, host));
    }

    @Override
    public List<Permission> toPermissions() {
        return getGrantedPrivileges()
            .stream()
            .map(Permission::instancePermission)
            .collect(Collectors.toList());
    }

    public List<String> listGrantedPrivileges() {
        List<String> privs = new ArrayList<>();

        List<String> grantedPrivilegesWithoutGrantOption = getPrivileges().entrySet()
            .stream()
            .filter(entry -> entry.getKey().hasSqlName())
            .filter(entry -> entry.getKey() != PrivilegeKind.GRANT_OPTION)
            .filter(Map.Entry::getValue)
            .map(Map.Entry::getKey)
            .map(PrivilegeKind::getSqlName)
            .collect(Collectors.toList());
        privs.addAll(grantedPrivilegesWithoutGrantOption);

        if (privs.isEmpty()) {
            privs.add("USAGE");
        }

        return privs;
    }
}

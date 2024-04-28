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

import com.google.common.base.Preconditions;
import com.taobao.tddl.common.privilege.AuthPlugin;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * User or role privilege info.
 *
 * @author shicai.xsc 2020/3/3 13:19
 * @see PolarAccount
 * @since 5.0.0.0
 */
public class PolarAccountInfo {
    private PolarAccount account;

    private PolarRolePrivilege rolePrivileges;
    private PolarInstPriv instPriv;
    private TreeMap<String, PolarDbPriv> dbPrivMap;
    private TreeMap<String, PolarTbPriv> tbPrivMap;

    public PolarAccountInfo(PolarAccount account) {
        Preconditions.checkNotNull(account, "Account can't be null!");
        this.account = account;
        this.rolePrivileges = new PolarRolePrivilege(account);
        this.instPriv = new PolarInstPriv();
        this.dbPrivMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.tbPrivMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    public static Collection<PolarAccountInfo> loadPolarAccounts(Connection conn, Collection<PolarAccount> accounts)
        throws SQLException {
        // Identifier to account info map
        Map<String, PolarAccountInfo> accountInfoMap = new HashMap<>(accounts.size());

        for (PolarAccountInfo instInfo : PolarInstPriv.loadInstPrivs(conn, accounts)) {
            accountInfoMap.put(instInfo.getIdentifier(), instInfo);
        }

        for (PolarAccountInfo dbInfo : PolarDbPriv.loadDbPrivs(conn, accounts)) {
            String identifier = dbInfo.getIdentifier();
            PolarAccountInfo info = accountInfoMap.get(identifier);
            if (info != null) {
                info.dbPrivMap = dbInfo.dbPrivMap;
            }
        }

        for (PolarAccountInfo tableInfo : PolarTbPriv.loadTbPrivs(conn, accounts)) {
            String identifier = tableInfo.getIdentifier();
            PolarAccountInfo info = accountInfoMap.get(identifier);
            if (info != null) {
                info.tbPrivMap = tableInfo.tbPrivMap;
            }
        }

        Collection<PolarRolePrivilege> rolePrivileges = PolarRolePrivilege.loadFromDb(conn, accountInfoMap.values()
            .stream()
            .map(PolarAccountInfo::getAccount)
            .collect(Collectors.toList()));

        for (PolarRolePrivilege rolePrivilege : rolePrivileges) {
            accountInfoMap.get(rolePrivilege.getUser().getIdentifier())
                .rolePrivileges = rolePrivilege;
        }

        return accountInfoMap.values();
    }

    public String getIdentifier() {
        return account.getIdentifier();
    }

    public boolean isMatch(String username, String host) {
        return account.matches(username, host);
    }

    public PolarDbPriv getDbPriv(String db) {
        return dbPrivMap.get(db.toLowerCase());
    }

    public PolarDbPriv getFirstDbPriv() {
        if (dbPrivMap.values().iterator().hasNext()) {
            return dbPrivMap.values().iterator().next();
        }
        return null;
    }

    public long getAccountId() {
        return getAccount().getAccountId();
    }

    public PolarTbPriv getTbPriv(String db, String tb) {
        String key = (db + "@" + tb).toLowerCase();
        return tbPrivMap.get(key);
    }

    public PolarTbPriv getFirstTbPriv() {
        if (tbPrivMap.values().iterator().hasNext()) {
            return tbPrivMap.values().iterator().next();
        }
        return null;
    }

    public void addDbPriv(PolarDbPriv dbPriv) {
        if (dbPriv != null && StringUtils.isNotBlank(dbPriv.getIdentifier())) {
            dbPrivMap.put(dbPriv.getIdentifier(), dbPriv);
        }
    }

    public void addTbPriv(PolarTbPriv tbPriv) {
        if (tbPriv != null && StringUtils.isNotBlank(tbPriv.getDbName()) && StringUtils
            .isNotBlank(tbPriv.getTbName())) {
            tbPrivMap.put(tbPriv.getIdentifier(), tbPriv);
        }
    }

    public PolarInstPriv getInstPriv() {
        return instPriv;
    }

    public void setInstPriv(PolarInstPriv instPriv) {
        this.instPriv = instPriv;
    }

    public PolarRolePrivilege getRolePrivileges() {
        return rolePrivileges;
    }

    public PolarAccount getAccount() {
        return this.account;
    }

    public String getUsername() {
        return getAccount().getUsername();
    }

    public String getHost() {
        return getAccount().getHost();
    }

    public String getPassword() {
        return getAccount().getPassword();
    }

    public AuthPlugin getAuthPlugin() {
        return getAccount().getAuthPlugin();
    }

    public void setPassword(String newPassword) {
        this.account = account.updatePassword(newPassword);
    }

    public AccountType getAccountType() {
        return getAccount().getAccountType();
    }

    public TreeMap<String, PolarDbPriv> getDbPrivMap() {
        return dbPrivMap;
    }

    public TreeMap<String, PolarTbPriv> getTbPrivMap() {
        return tbPrivMap;
    }

    public void clearAllPrivileges() {
        instPriv.revokeAllPrivileges();
        dbPrivMap.clear();
        tbPrivMap.clear();
        rolePrivileges.deleteAllRoles();
    }

    public boolean canCover(PolarAccountInfo grantee, PrivManageLevel level, boolean checkGrant) {
        boolean res = false;
        PolarDbPriv dbPriv = null;
        PolarTbPriv tbPriv = null;

        switch (level) {
        case INST:
            return instPriv.canCover(grantee.getInstPriv(), checkGrant);
        case DB:
            PolarDbPriv granteeDbPriv = grantee.getFirstDbPriv();
            dbPriv = getDbPriv(granteeDbPriv.getIdentifier());
            res |= instPriv.canCover(granteeDbPriv, checkGrant);
            res |= dbPriv != null && dbPriv.canCover(granteeDbPriv, checkGrant);
            return res;
        case TABLE:
            PolarTbPriv granteeTbPriv = grantee.getFirstTbPriv();
            dbPriv = getDbPriv(granteeTbPriv.getDbName());
            tbPriv = getTbPriv(granteeTbPriv.getDbName(), granteeTbPriv.getTbName());
            res |= instPriv.canCover(granteeTbPriv, checkGrant);
            res |= dbPriv != null && dbPriv.canCover(granteeTbPriv, checkGrant);
            res |= tbPriv != null && tbPriv.canCover(granteeTbPriv, checkGrant);
            return res;
        default:
            return res;
        }
    }

    public boolean hasUsageOnDb(String db) {
        if (instPriv.hasUsagePriv()) {
            return true;
        } else {
            if (dbPrivMap.containsKey(db) && dbPrivMap.get(db).hasUsagePriv()) {
                return true;
            }

            for (String dbTb : tbPrivMap.keySet()) {
                String privDb = dbTb.split("@")[0];
                if (StringUtils.equalsIgnoreCase(privDb, db) && tbPrivMap.get(dbTb).hasUsagePriv()) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean hasUsageOnTb(String db, String tb) {
        if (instPriv.hasUsagePriv()) {
            return true;
        } else {
            if (dbPrivMap.containsKey(db) && dbPrivMap.get(db).hasUsagePriv()) {
                return true;
            }

            for (String dbTb : tbPrivMap.keySet()) {
                String privDb = dbTb.split("@")[0];
                String privTb = dbTb.split("@")[1];
                if (StringUtils.equalsIgnoreCase(privDb, db) && StringUtils.equalsIgnoreCase(privTb, tb)
                    && tbPrivMap.get(dbTb).hasUsagePriv()) {
                    return true;
                }
            }
        }

        return false;
    }

    public boolean canGrantOrRevokeRole(PolarAccountInfo role) {
        if (instPriv.hasPrivilege(PrivilegeKind.CREATE_USER)) {
            return true;
        }

        // TODO: Implement nest lookup
        return rolePrivileges.hasAdminPermissionOf(role.getAccount().getAccountId());
    }

    public void checkRoleGranted(PolarAccount role) {
        rolePrivileges.checkRoleGranted(role);
    }

    public List<String> showGrants(PolarPrivilegeData accountPrivilegeData) {
        List<String> result =
            new ArrayList<>(getTbPrivMap().size() + getDbPrivMap().size() + 2);
        getInstPriv().showGrantsResult(getAccount())
            .ifPresent(result::add);

        getDbPrivMap().values()
            .stream()
            .filter(db -> !StringUtils.equalsIgnoreCase(db.getDbName(), PolarPrivUtil.INFORMATION_SCHEMA))
            .filter(dbPrivilege -> dbPrivilege.hasPrivilege(PrivilegeKind.GRANT_OPTION))
            .map(dbPrivilege -> dbPrivilege.showGrantsResult(getAccount()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(result::add);

        getDbPrivMap().values()
            .stream()
            .filter(db -> !StringUtils.equalsIgnoreCase(db.getDbName(), PolarPrivUtil.INFORMATION_SCHEMA))
            .filter(dbPrivilege -> !dbPrivilege.hasPrivilege(PrivilegeKind.GRANT_OPTION))
            .map(dbPrivilege -> dbPrivilege.showGrantsResult(getAccount()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(result::add);

        getTbPrivMap().values()
            .stream()
            .filter(tablePrivilege -> tablePrivilege.hasPrivilege(PrivilegeKind.GRANT_OPTION))
            .map(tablePrivilege -> tablePrivilege.showGrantsResult(getAccount()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(result::add);

        getTbPrivMap().values()
            .stream()
            .filter(tablePrivilege -> !tablePrivilege.hasPrivilege(PrivilegeKind.GRANT_OPTION))
            .map(tablePrivilege -> tablePrivilege.showGrantsResult(getAccount()))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .forEach(result::add);

        result.addAll(getRolePrivileges().showGrantResult(accountPrivilegeData));

        return result;
    }

    public PolarAccountInfo deepCopy() {
        PolarAccountInfo result = new PolarAccountInfo(this.getAccount().deepCopy());
        result.setInstPriv(getInstPriv().deepCopy());

        getDbPrivMap().values()
            .stream()
            .map(PolarDbPriv::deepCopy)
            .forEach(result::addDbPriv);

        getTbPrivMap().values()
            .stream()
            .map(PolarTbPriv::deepCopy)
            .forEach(result::addTbPriv);

        result.rolePrivileges = getRolePrivileges().deepCopy();

        return result;
    }

    public void mergeUserPrivileges(PolarAccountInfo other) {
        instPriv.mergePriv(other.instPriv, PrivManageType.GRANT_PRIVILEGE);

        for (String dbKey : other.getDbPrivMap().keySet()) {
            PolarDbPriv curDbPriv = dbPrivMap.get(dbKey);
            PolarDbPriv otherDbPriv = other.dbPrivMap.get(dbKey);
            if (curDbPriv == null) {
                dbPrivMap.put(dbKey, otherDbPriv);
            } else {
                curDbPriv.mergePriv(otherDbPriv, PrivManageType.GRANT_PRIVILEGE);
            }
        }

        for (String tableKey : other.getTbPrivMap().keySet()) {
            PolarTbPriv curTablePriv = tbPrivMap.get(tableKey);
            PolarTbPriv otherTablePriv = other.tbPrivMap.get(tableKey);

            if (curTablePriv == null) {
                tbPrivMap.put(tableKey, otherTablePriv);
            } else {
                curTablePriv.mergePriv(otherTablePriv, PrivManageType.GRANT_PRIVILEGE);
            }
        }
    }

    public Collection<String> toUpdatePrivilegeSqls(boolean grant) {
        List<String> ret = new ArrayList<>(3);
        instPriv.toUpdatePrivilegeSql(grant).ifPresent(ret::add);

        for (PolarDbPriv dbPriv : dbPrivMap.values()) {
            ret.add(dbPriv.toInsertNewSql());
            dbPriv.toUpdatePrivilegeSql(grant)
                .ifPresent(ret::add);
        }

        for (PolarTbPriv tbPriv : tbPrivMap.values()) {
            ret.add(tbPriv.toInsertNewSql());
            tbPriv.toUpdatePrivilegeSql(grant)
                .ifPresent(ret::add);
        }

        return ret;
    }

    public List<Permission> toPermissions() {
        List<Permission> permissions = new ArrayList<>(instPriv.toPermissions());

        dbPrivMap.values()
            .stream()
            .map(PolarDbPriv::toPermissions)
            .forEach(permissions::addAll);

        tbPrivMap.values()
            .stream()
            .map(PolarTbPriv::toPermissions)
            .forEach(permissions::addAll);

        return permissions;
    }

    public void addGrantOptionToAll() {
        if (instPriv.hasAnyPrivilege()) {
            instPriv.grantPrivilege(PrivilegeKind.GRANT_OPTION);
        }

        dbPrivMap.values()
            .stream()
            .filter(PolarDbPriv::hasAnyPrivilege)
            .forEach(dbPriv -> dbPriv.grantPrivilege(PrivilegeKind.GRANT_OPTION));

        tbPrivMap.values()
            .stream()
            .filter(PolarTbPriv::hasAnyPrivilege)
            .forEach(tbPrivMap -> tbPrivMap.grantPrivilege(PrivilegeKind.GRANT_OPTION));
    }

    private static boolean checkSpecialCases(Permission permission) {
        // For information schema everyone only has select permission.
        if (PolarPrivUtil.INFORMATION_SCHEMA.equalsIgnoreCase(permission.getDatabase())) {
            return PrivilegeKind.SELECT == permission.getPrivilege();
        }

        return true;
    }

}

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

import com.google.common.collect.Maps;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author shicai.xsc 2020/3/10 14:17
 * @since 5.0.0.0
 */
public abstract class BasePolarPriv {

    public static final String SELECT = "SELECT";
    public static final String INSERT = "INSERT";
    public static final String UPDATE = "UPDATE";
    public static final String DELETE = "DELETE";
    public static final String CREATE = "CREATE";
    public static final String DROP = "DROP";
    public static final String INDEX = "INDEX";
    public static final String ALTER = "ALTER";
    public static final String SHOW_VIEW = "SHOW VIEW";
    public static final String CREATE_VIEW = "CREATE VIEW";
    public static final String CREATE_USER = "CREATE USER";
    public static final String ALL_PRIVILEGES = "ALL PRIVILEGES";
    public static final String ALL = "ALL";
    public static final String GRANT_OPTION = "GRANT OPTION";

    protected String userName;
    protected String host;
//    protected boolean selectPriv;
//    protected boolean insertPriv;
//    protected boolean updatePriv;
//    protected boolean deletePriv;
//    protected boolean createPriv;
//    protected boolean dropPriv;
//    protected boolean grantPriv;
//    protected boolean indexPriv;
//    protected boolean alterPriv;
//    protected boolean showViewPriv;
//    protected boolean createViewPriv;

    private final PrivilegeScope scope;
    protected final EnumMap<PrivilegeKind, Boolean> privileges = new EnumMap<>(PrivilegeKind.class);

    public BasePolarPriv(PrivilegeScope scope) {
        this.scope = scope;
    }

    static <T extends BasePolarPriv> void loadBasePriv(ResultSet rs, T privilege) throws SQLException {
        privilege.setUserName(rs.getString(PolarPrivUtil.USER_NAME));
        privilege.setHost(rs.getString(PolarPrivUtil.HOST));

        for (PrivilegeKind kind : PrivilegeKind.kindsByScope(privilege.getScope())) {
            privilege.privileges.put(kind, rs.getInt(kind.getColumnName()) == 1);
        }
    }

    public String getIdentifier() {
        return "";
    }

    public static void copy(BasePolarPriv src, BasePolarPriv dst) {
        dst.userName = src.userName;
        dst.host = src.host;
        dst.privileges.clear();
        dst.privileges.putAll(src.privileges);
    }

    public void copyFrom(BasePolarPriv src) {
        copy(src, this);
    }

    public void updatePrivilege(String privStr, boolean value) {
        if (PrivilegeKind.isAll(privStr)) {
            // GRANT ALL should not contains grant options
            allPrivilegeKinds()
                .stream()
                .filter(kind -> kind != PrivilegeKind.GRANT_OPTION)
                .forEach(kind -> privileges.put(kind, value));
        } else {
            privileges.put(PrivilegeKind.kindsByName(privStr), value);
        }
    }

    public void revokeAllPrivileges() {
        privileges.clear();
    }

    public Collection<PrivilegeKind> allPrivilegeKinds() {
        return PrivilegeKind.kindsByScope(scope);
    }

//    public void setAllPrivsWithoutGrant(boolean value) {
//        this.selectPriPv = value;
//        this.insertPriv = value;
//        this.updatePriv = value;
//        this.deletePriv = value;
//        this.createPriv = value;
//        this.dropPriv = value;
//        this.indexPriv = value;
//        this.alterPriv = value;
//        this.showViewPriv = value;
//        this.createViewPriv = value;
//    }

    public boolean hasPrivilege(PrivilegeKind kind) {
        return Optional.ofNullable(privileges.get(kind))
            .orElse(false);
    }

    public void revokePrivilege(PrivilegeKind kind) {
        privileges.remove(kind);
    }

    public void grantPrivilege(PrivilegeKind kind) {
        privileges.put(kind, true);
    }

    public boolean canCover(BasePolarPriv privToCover, boolean checkGrant) {
        // check if current priv can cover all the privileges of other
        for (PrivilegeKind kind : privToCover.privileges.keySet()) {
            if (kind == PrivilegeKind.GRANT_OPTION) {
                continue;
            }

            if (privToCover.hasPrivilege(kind) && !this.hasPrivilege(kind)) {
                return false;
            }
        }

        return !checkGrant || hasPrivilege(PrivilegeKind.GRANT_OPTION);
    }

    public void mergePriv(BasePolarPriv toMerge, PrivManageType type) {
        if (type == PrivManageType.REVOKE_PRIVILEGE) {
            toMerge.getPrivileges()
                .entrySet()
                .stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .forEach(this::revokePrivilege);
        } else if (type == PrivManageType.GRANT_PRIVILEGE) {
            toMerge.getPrivileges()
                .entrySet()
                .stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .forEach(this::grantPrivilege);
        }
    }

    public boolean hasAnyPrivilege() {
        return allPrivilegeKinds().stream().anyMatch(this::hasPrivilege);
    }

    public boolean hasAnyPrivilegeWithoutGrantOption() {
        return allPrivilegeKinds()
            .stream()
            .filter(privilege -> privilege != PrivilegeKind.GRANT_OPTION)
            .anyMatch(this::hasPrivilege);
    }

    public boolean hasUsagePriv() {
        return hasAnyPrivilege();
    }

    public boolean hasAllPrivs() {
        return allPrivilegeKinds()
            .stream()
            .allMatch(this::hasPrivilege);
    }

    private boolean hasAllPrivilegesWithoutGrantOption() {
        return allPrivilegeKinds()
            .stream()
            .filter(privilege -> privilege != PrivilegeKind.GRANT_OPTION)
            .filter(PrivilegeKind::hasSqlName)
            .allMatch(this::hasPrivilege);
    }

    public List<String> getGrantedPrivs() {
        return getPrivileges().entrySet()
            .stream()
            .filter(Map.Entry::getValue)
            .map(Map.Entry::getKey)
            .map(PrivilegeKind::getSqlName)
            .collect(Collectors.toList());
    }

    protected Optional<String> showGrantsResult(PolarAccount account, String target, boolean usageOnEmpty) {
        StringBuilder sb = new StringBuilder();

        String privilegeString;

        List<String> grantedPrivilegesWithoutGrantOption = getPrivileges().entrySet()
            .stream()
            .filter(entry -> entry.getKey().hasSqlName())
            .filter(entry -> entry.getKey() != PrivilegeKind.GRANT_OPTION)
            .filter(Map.Entry::getValue)
            .map(Map.Entry::getKey)
            .map(PrivilegeKind::getSqlName)
            .collect(Collectors.toList());

        if (grantedPrivilegesWithoutGrantOption.isEmpty()) {
            if (!usageOnEmpty && !hasPrivilege(PrivilegeKind.GRANT_OPTION)) {
                return Optional.empty();
            } else {
                privilegeString = "USAGE";
            }
        } else if (hasAllPrivilegesWithoutGrantOption()) {
            privilegeString = PrivilegeKind.SQL_ALL_PRIVILEGES;
        } else {
            privilegeString = String.join(", ", grantedPrivilegesWithoutGrantOption);
        }

        sb.append(String.format("GRANT %s ON %s TO %s", privilegeString, target,
            account.getIdentifier()));

        if (this.hasPrivilege(PrivilegeKind.GRANT_OPTION)) {
            sb.append(" WITH GRANT OPTION");
        }

        return Optional.of(sb.toString());
    }

    public String getUserName() {
        return userName;
    }

    public String getHost() {
        return host;
    }

    public PrivilegeScope getScope() {
        return scope;
    }

    public Map<PrivilegeKind, Boolean> getPrivileges() {
        return Maps.immutableEnumMap(privileges);
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Collection<PrivilegeKind> getGrantedPrivileges() {
        return getPrivileges().keySet()
            .stream()
            .filter(this::hasPrivilege)
            .collect(Collectors.toList());
    }

    public Optional<String> toSetPrivilegeSql(boolean grant) {
        Collection<PrivilegeKind> privileges = getGrantedPrivileges();
        if (privileges.size() == 0) {
            return Optional.empty();
        } else {
            return Optional.of(privileges.stream()
                .map(kind -> String.format("`%s` = %d ", kind.getColumnName(), grant ? 1 : 0))
                .collect(Collectors.joining(",")));
        }
    }

    public abstract String toInsertNewSql();

    public abstract List<Permission> toPermissions();
}

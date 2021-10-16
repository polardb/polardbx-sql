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

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetDefaultRoleStatement;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Set of roles a user/role granted.
 * <p>
 * A role is a collections of privileges. Roles can be granted/revoked to/from users/roles. This class is not thread
 * safe, and it's guarded by {@link PolarPrivManager}.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/roles.html">Roles</a>
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/grant.html">Grant Roles</a>
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/revoke.html">Revoke Roles</a>
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-default-role.html">Set Default Role</a>
 * @since 5.4.9
 */
public class PolarRolePrivilege {
    /**
     * Column names for table <code>`role_priv`</code>
     */
    private static final String TABLE_ROLE_PRIV = "role_priv";
    private static final String COL_ROLE_ID = "role_id";
    private static final String COL_RECEIVER_ID = "receiver_id";
    private static final String COL_IS_WITH_ADMIN_OPTION = "with_admin_option";
    private static final String COL_IS_DEFAULT_ROLE = "default_role";

    /**
     * Column names for table <code>`default_role_state`</code>
     */
    private static final String TABLE_DEFAULT_ROLE_STATE = "default_role_state";
    private static final String COL_ACCOUNT_ID = "account_id";
    private static final String COL_DEFAULT_ROLE_STATE = "default_role_state";

    private static final Logger LOG = LoggerFactory.getLogger(PolarRolePrivilege.class);

    /**
     * Owner of this privilege.
     */
    private final PolarAccount user;

    /**
     * Mappings from role id to admin option.
     * <p>
     * Key is role, value is admin option.
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/grant.html#grant-roles">Grant Role</a>
     */
    private final Map<Long, GrantedRoleInfo> mapping = new HashMap<>(128);

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-default-role.html">Set Default Role</a>
     */
    private DefaultRoleState defaultRoleState = DefaultRoleState.NONE;

    public PolarRolePrivilege(PolarAccount user) {
        this.user = user;
    }

    public static Collection<PolarRolePrivilege> loadFromDb(Connection conn, Collection<PolarAccount> accounts)
        throws SQLException {
        Map<Long, PolarRolePrivilege> ret = new HashMap<>(accounts.size());
        accounts.forEach(acc -> ret.computeIfAbsent(acc.getAccountId(), id -> new PolarRolePrivilege(acc)));

        loadGrantedRolesFromDb(conn, ret);
        loadDefaultRoleFromDb(conn, ret);
        return ret.values();
    }

    private static void loadGrantedRolesFromDb(Connection conn, Map<Long, PolarRolePrivilege> data)
        throws SQLException {
        final String sql = String.format("SELECT %s, %s, %s, %s from %s where %s = ?;",
            COL_ROLE_ID, COL_RECEIVER_ID, COL_IS_WITH_ADMIN_OPTION, COL_IS_DEFAULT_ROLE, TABLE_ROLE_PRIV,
            COL_RECEIVER_ID);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Long accountId : data.keySet()) {
                stmt.setLong(1, accountId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        updateMapping(rs, receiverId -> Optional.of(data.get(accountId)));
                    }
                }
            }
        }
    }

    private static void loadDefaultRoleFromDb(Connection conn, Map<Long, PolarRolePrivilege> data) throws SQLException {
        final String sql = String.format("SELECT %s, %s FROM %s WHERE %s = ?;",
            COL_ACCOUNT_ID, COL_DEFAULT_ROLE_STATE, TABLE_DEFAULT_ROLE_STATE, COL_ACCOUNT_ID);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (Long accountId : data.keySet()) {
                stmt.setLong(1, accountId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        updateDefaultRoleState(rs, receiverId -> Optional.of(data.get(receiverId)));
                    }
                }
            }
        }
    }

    /**
     * Loads role related info from metadb, including granted role info, user's default role state
     *
     * @param conn Metadb connection.
     * @param userInfoData Mapping from account id to account info.
     * @see PolarAccountInfo
     */
    public static void loadFromDb(Connection conn, PolarPrivilegeData userInfoData)
        throws SQLException {
        loadGrantedRolesFromDb(conn, userInfoData);
        loadDefaultRoleStateFromDb(conn, userInfoData);
    }

    private static void loadGrantedRolesFromDb(Connection conn, PolarPrivilegeData userInfoData)
        throws SQLException {
        final String sql = String.format("SELECT %s, %s, %s, %s from %s;",
            COL_ROLE_ID, COL_RECEIVER_ID, COL_IS_WITH_ADMIN_OPTION, COL_IS_DEFAULT_ROLE, TABLE_ROLE_PRIV);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    updateMapping(rs,
                        receiverId -> Optional.ofNullable(userInfoData.getById(receiverId))
                            .map(PolarAccountInfo::getRolePrivileges));
                }
            }
        }
    }

    private static void updateMapping(ResultSet rs, Function<Long, Optional<PolarRolePrivilege>> dataProvider)
        throws SQLException {
        long granteeId = rs.getLong(COL_ROLE_ID);
        long receiverId = rs.getLong(COL_RECEIVER_ID);

        GrantedRoleInfo info = new GrantedRoleInfo();
        info.withAdminOption = rs.getBoolean(COL_IS_WITH_ADMIN_OPTION);
        info.defaultRole = rs.getBoolean(COL_IS_DEFAULT_ROLE);

        dataProvider.apply(receiverId)
            .ifPresent(data -> data.mapping.put(granteeId, info));
    }

    private static void loadDefaultRoleStateFromDb(Connection conn, PolarPrivilegeData userInfoData)
        throws SQLException {
        final String sql = String.format("SELECT %s, %s from %s;",
            COL_ACCOUNT_ID, COL_DEFAULT_ROLE_STATE, TABLE_DEFAULT_ROLE_STATE);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    updateDefaultRoleState(rs,
                        accountId -> Optional.ofNullable(userInfoData.getById(accountId)).map(
                            PolarAccountInfo::getRolePrivileges));
                }
            }
        }
    }

    private static void updateDefaultRoleState(ResultSet rs,
                                               Function<Long, Optional<PolarRolePrivilege>> dataProvider)
        throws SQLException {
        long accountId = rs.getLong(COL_ACCOUNT_ID);
        byte stateId = rs.getByte(COL_DEFAULT_ROLE_STATE);

        dataProvider.apply(accountId)
            .ifPresent(s -> s.defaultRoleState = DefaultRoleState.lookupById(stateId));
    }

    /**
     * Insert role privileges into metadb.
     * <p>
     * If a role has been granted to an account, it's ignored, even they have different admin options.
     *
     * @param conn Metadb connection.
     * @param grantees Roles granted.
     * @param receivers Accounts who receive these roles.
     * @param withAdminOption Whether has admin option.
     */
    public static void syncGrantRolePrivilegesToDb(Connection conn,
                                                   List<PolarAccountInfo> grantees,
                                                   List<PolarAccountInfo> receivers,
                                                   boolean withAdminOption) {
        final String sql = String.format("insert ignore into %s (%s, %s, %s) values (?, ?, ?);", TABLE_ROLE_PRIV,
            COL_ROLE_ID, COL_RECEIVER_ID, COL_IS_WITH_ADMIN_OPTION);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int updateRows = 0;
            for (PolarAccountInfo grantee : grantees) {
                for (PolarAccountInfo receiver : receivers) {
                    stmt.setLong(1, grantee.getAccount().getAccountId());
                    stmt.setLong(2, receiver.getAccount().getAccountId());
                    stmt.setBoolean(3, withAdminOption);
                    updateRows += stmt.executeUpdate();
                }
            }
            int expectedRowCount = grantees.size() * receivers.size();
            LOG.info("Start to insert role privileges into metadb.");
            LOG.info("Finished insert role privileges into metadb, expected row count: " + expectedRowCount + ", "
                + "actual row count: " + updateRows);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SERVER, e, "Failed to sync role privileges to metadb.");
        }
    }

    /**
     * Delete role privileges from metadb.
     *
     * @param conn Metadb connection.
     * @param roles Roles to be revoked.
     * @param fromAccounts Roles to be revoked from.
     */
    public static void syncRevokeRolePrivilegesToDb(Connection conn,
                                                    List<PolarAccountInfo> roles,
                                                    List<PolarAccountInfo> fromAccounts) {
        final String sql = String.format("delete from %s where %s = ? and %s = ?;", TABLE_ROLE_PRIV, COL_ROLE_ID,
            COL_RECEIVER_ID);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int updateRows = 0;
            for (PolarAccountInfo role : roles) {
                for (PolarAccountInfo fromAccount : fromAccounts) {
                    stmt.setLong(1, role.getAccount().getAccountId());
                    stmt.setLong(2, fromAccount.getAccount().getAccountId());
                    updateRows += stmt.executeUpdate();
                }
            }

            int expectedRowCount = roles.size() * fromAccounts.size();
            LOG.info("Start to delete role privileges from metadb.");
            LOG.info("Finished deleteing role privileges from metadb, expected row count: " + expectedRowCount + ", "
                + "actual row count: " + updateRows);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SERVER, e, "Failed to sync role privileges to metadb.");
        }
    }

    /**
     * Update default roles in metadb.
     *
     * @param conn Metadb connection.
     * @param accounts Accounts to update.
     * @param state When equals to {@link DefaultRoleState#ROLES}, update {@value TABLE_ROLE_PRIV}'s {@value
     * COL_IS_DEFAULT_ROLE} column and {@value TABLE_DEFAULT_ROLE_STATE}.
     * <p>Otherwise only update {@value TABLE_DEFAULT_ROLE_STATE}.</p>
     * @param defaultRoles Can't be empty when {@code state} is {@link DefaultRoleState#ROLES}
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-default-role.html">Set Default Role</a>
     */
    public static void syncDefaultRolesToDb(Connection conn, List<PolarAccountInfo> accounts,
                                            DefaultRoleState state,
                                            List<PolarAccountInfo> defaultRoles) {
        try {
            switch (state) {
            case NONE:
            case ALL:
                syncDefaultRoleStatesToDb(conn, accounts, state);
                break;
            case ROLES:
                syncDefaultRoleStatesToDb(conn, accounts, state);
                syncRoleDefaultOptionToDb(conn, accounts, defaultRoles);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized default role state: " + state);
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SERVER, e, "Failed to sync default role option to metadb.");
        }
    }

    private static void syncDefaultRoleStatesToDb(Connection conn, List<PolarAccountInfo> accounts,
                                                  DefaultRoleState state) throws SQLException {
        final String sql = String.format("insert into %s(%s, %s) values (?, ?) "
                + "on duplicate key update %s = ?;",
            TABLE_DEFAULT_ROLE_STATE, COL_ACCOUNT_ID,
            COL_DEFAULT_ROLE_STATE, COL_ACCOUNT_ID);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int updatedCount = 0;
            for (PolarAccountInfo account : accounts) {
                stmt.setLong(1, account.getAccount().getAccountId());
                stmt.setByte(2, state.stateId);
                stmt.setLong(3, account.getAccount().getAccountId());
                updatedCount += stmt.executeUpdate();
            }

            LOG.info("Start to update default role state from metadb.");
            int expectedRowCount = accounts.size();
            LOG.info("Finished updating default role state in metadb, expected row count: " + expectedRowCount + ", "
                + "actual row count: " + updatedCount);
        }
    }

    private static void syncRoleDefaultOptionToDb(Connection conn, List<PolarAccountInfo> accounts,
                                                  List<PolarAccountInfo> roles) throws SQLException {
        final String sql = String.format("update %s set %s = true where %s = ? and %s = ?;", TABLE_ROLE_PRIV,
            COL_IS_DEFAULT_ROLE, COL_RECEIVER_ID, COL_ROLE_ID);

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (PolarAccountInfo account : accounts) {
                for (PolarAccountInfo role : roles) {
                    stmt.setLong(1, account.getAccount().getAccountId());
                    stmt.setLong(2, role.getAccount().getAccountId());
                    stmt.executeUpdate();
                }
            }

            LOG.info("Start to update default role option from metadb.");
            int expectedRowCount = accounts.size() * roles.size();
            int updatedCount = Arrays.stream(stmt.executeBatch()).sum();
            LOG.info("Finished upating default role option in metadb, expected row count: " + expectedRowCount + ", "
                + "actual row count: " + updatedCount);
        }
    }

    /**
     * Drop role related data when dropping accounts.
     *
     * <p>
     * It first deletes data from {@value TABLE_DEFAULT_ROLE_STATE}. Then delete data from {@value TABLE_ROLE_PRIV}.
     * </p>
     *
     * @param conn Metadb connection.
     * @param accountIds Account ids to delete.
     */
    public static void dropAccounts(Connection conn, Collection<Long> accountIds) throws SQLException {
        dropRolePrivileges(conn, accountIds);
        dropDefaultRoleStates(conn, accountIds);
    }

    static void dropDefaultRoleStates(Connection conn, Collection<Long> accountIds) throws SQLException {
        final String sql = String.format("delete from %s where %s = ?", TABLE_DEFAULT_ROLE_STATE, COL_ACCOUNT_ID);
        try (PreparedStatement stat = conn.prepareStatement(sql)) {
            for (long accountId : accountIds) {
                stat.setLong(1, accountId);
                stat.executeUpdate();
            }
        }
    }

    static void dropRolePrivileges(Connection conn, Collection<Long> accountIds) throws SQLException {
        final String sql = String.format("delete from %s where %s = ? or %s = ?", TABLE_ROLE_PRIV, COL_ROLE_ID,
            COL_RECEIVER_ID);
        try (PreparedStatement stat = conn.prepareStatement(sql)) {
            for (long accountId : accountIds) {
                stat.setLong(1, accountId);
                stat.setLong(2, accountId);
                stat.executeUpdate();
            }
        }
    }

    /**
     * Test whether can grant this role to others.
     *
     * @param roleId Role id to be granted.
     * @return Ture if current account has been granted this role with admin option, else false.
     */
    public boolean hasAdminPermissionOf(Long roleId) {
        return Optional.ofNullable(mapping.get(roleId))
            .map(info -> info.withAdminOption)
            .orElse(false);
    }

    /**
     * Add roles to in memory mapping. If a role already exists in mapping, it's ignored. This behavior is align with
     * {@link #syncGrantRolePrivilegesToDb}
     *
     * @param roles Roles to be add.
     * @param withAdminOption With admin option.
     */
    public void addRoles(List<PolarAccountInfo> roles, boolean withAdminOption) {
        for (PolarAccountInfo role : roles) {
            GrantedRoleInfo info = new GrantedRoleInfo();
            info.withAdminOption = withAdminOption;
            mapping.putIfAbsent(role.getAccount().getAccountId(), info);
        }
    }

    /**
     * Delete roles from in memory mapping.
     *
     * @param roles Roles to be deleted.
     * @see #syncRevokeRolePrivilegesToDb
     */
    public void deleteRoles(List<PolarAccountInfo> roles) {
        roles.stream()
            .map(r -> r.getAccount().getAccountId())
            .forEach(mapping::remove);
    }

    public void deleteAllRoles() {
        mapping.clear();
    }

    /**
     * Clear current roles and replace them with new roles without admin option.
     *
     * @param newRoles Roles that will be replaced.
     */
    public void replaceRoles(List<PolarAccountInfo> newRoles) {
        mapping.clear();
        addRoles(newRoles, false);
    }

    /**
     * Display role part of show granted.
     *
     * @param accountPrivilegeData Mapping from account id to account info.
     * @return A string to display granted roles, empty if no roles granted.
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/show-grants.html">Show Grants</a>
     */
    public Collection<String> showGrantResult(PolarPrivilegeData accountPrivilegeData) {
        if (mapping.isEmpty()) {
            return Collections.emptyList();
        }

        List<String> ret = new ArrayList<>(2);

        String withAdminRolesString = mapping.entrySet()
            .stream()
            .filter(entry -> entry.getValue().withAdminOption)
            .map(Map.Entry::getKey)
            .sorted()
            .map(accountPrivilegeData::getById)
            .filter(Objects::nonNull)
            .map(PolarAccountInfo::getIdentifier)
            .collect(Collectors.joining(", "));

        if (!withAdminRolesString.isEmpty()) {
            ret.add(String.format("GRANT %s TO %s WITH ADMIN OPTION", withAdminRolesString, user.getIdentifier()));
        }

        String withoutAdminRolesString = mapping.entrySet()
            .stream()
            .filter(entry -> !entry.getValue().withAdminOption)
            .map(Map.Entry::getKey)
            .sorted()
            .map(accountPrivilegeData::getById)
            .filter(Objects::nonNull)
            .map(PolarAccountInfo::getIdentifier)
            .collect(Collectors.joining(", "));

        if (!withoutAdminRolesString.isEmpty()) {
            ret.add(String.format("GRANT %s TO %s", withoutAdminRolesString, user.getIdentifier()));
        }

        return ret;
    }

    /**
     * Check whether role has bee granted to current user, and throws exception if not.
     *
     * @param role Role to check.
     */
    public void checkRoleGranted(PolarAccount role) {
        if (!mapping.containsKey(role.getAccountId())) {
            throw new TddlRuntimeException(ErrorCode.ERR_ROLE_NOT_GRANTED, role.getIdentifier(), user.getIdentifier());
        }
    }

    public PolarAccount getUser() {
        return user;
    }

    /**
     * Check whether all roles has been granted to this user, and throws exceptions if not.
     *
     * @param roles Roles to check.
     */
    public void checkRolesGranted(Stream<PolarAccount> roles) {
        roles.forEach(this::checkRoleGranted);
    }

    /**
     * Update default role in memory data.
     *
     * @param newDefaultRoleState New default role state.
     * @param roles Set to default if new state is {@link DefaultRoleState#ROLES}.
     * @see #syncDefaultRoleStatesToDb
     */
    public void updateDefaultRole(DefaultRoleState newDefaultRoleState, List<PolarAccountInfo> roles) {
        Preconditions.checkNotNull(newDefaultRoleState, "Default role state can't be null!");
        Preconditions.checkNotNull(roles, "Roles can't be null!");

        this.defaultRoleState = newDefaultRoleState;

        if (DefaultRoleState.ROLES == newDefaultRoleState) {
            for (PolarAccountInfo role : roles) {
                mapping.get(role.getAccountId()).defaultRole = true;
            }
        }
    }

    /**
     * If <code>newSpec</code> is {@link ActiveRoles.ActiveRoleSpec#ROLES} or {@link
     * ActiveRoles.ActiveRoleSpec#ALL_EXCEPT}, this method validates input roles are granted to this account.
     *
     * @param newSpec New active role spce.
     * @param roles For {@link ActiveRoles.ActiveRoleSpec#ROLES} or {@link ActiveRoles.ActiveRoleSpec#ALL_EXCEPT}.
     */
    public ActiveRoles checkAndGetActiveRoles(ActiveRoles.ActiveRoleSpec newSpec, List<PolarAccountInfo> roles) {
        Preconditions.checkNotNull(newSpec, "Active role spec can't be null!");
        Preconditions.checkNotNull(roles, "Roles can't be null!");

        if (newSpec == ActiveRoles.ActiveRoleSpec.ALL_EXCEPT || newSpec == ActiveRoles.ActiveRoleSpec.ROLES) {
            checkRolesGranted(roles.stream().map(PolarAccountInfo::getAccount));
        }

        return new ActiveRoles(newSpec, roles.stream().map(PolarAccountInfo::getAccountId).collect(Collectors.toSet()));
    }

    /**
     * Return this account's active role ids according to active role spec.
     *
     * @param activeRoles Active role spec.
     * @param mandatoryRoles System's mandatory roles.
     * @return Active role ids.
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html">Set Role</a>
     * @see PolarPrivilegeConfig
     */
    public Collection<Long> getActiveRoleIds(ActiveRoles activeRoles, List<Long> mandatoryRoles) {
        Preconditions.checkNotNull(activeRoles, "Active roles can't be null!");
        Preconditions.checkNotNull(mandatoryRoles, "Mandatory roles can't be null!");
        Collection<Long> activeRoleIds;
        switch (activeRoles.getActiveRoleSpec()) {
        case NONE:
            activeRoleIds = Collections.emptyList();
            break;
        case ALL:
            activeRoleIds = CollectionUtils.union(mapping.keySet(), mandatoryRoles);
            break;
        case ALL_EXCEPT:
            activeRoleIds = CollectionUtils.subtract(CollectionUtils.union(mapping.keySet(), mandatoryRoles),
                activeRoles.getRoles());
            break;
        case ROLES:
            activeRoleIds = activeRoles.getRoles();
            break;
        case DEFAULT:
            activeRoleIds = getDefaultRoles(mandatoryRoles);
            break;
        default:
            throw new IllegalArgumentException("Unrecognized active role spec: " + activeRoles.getActiveRoleSpec());
        }

        return activeRoleIds.stream()
            .filter(this.mapping::containsKey)
            .collect(Collectors.toList());
    }

    /**
     * Get default roles including mandatory roles.
     *
     * @param mandatoryRoles Mandatory roles
     * @return Default role ids.
     * @see DefaultRoleState
     */
    public Collection<Long> getDefaultRoles(List<Long> mandatoryRoles) {
        switch (defaultRoleState) {
        case NONE:
            return Collections.emptyList();
        case ALL:
            return getAllRoles(mandatoryRoles);
        case ROLES:
            return mapping.entrySet().stream().filter(e -> e.getValue().defaultRole)
                .map(Map.Entry::getKey).collect(Collectors.toList());
        default:
            throw new IllegalArgumentException("Unrecognized default role state: " + defaultRoleState);
        }
    }

    /**
     * All granted roles with mandatory roles.
     *
     * @param mandatoryRoles Mandatory roles.
     * @return All granted roles with mandatory roles.
     */
    public Collection<Long> getAllRoles(List<Long> mandatoryRoles) {
        return CollectionUtils.union(mapping.keySet(), mandatoryRoles);
    }

    public PolarRolePrivilege deepCopy() {
        PolarRolePrivilege copyed = new PolarRolePrivilege(user.deepCopy());
        this.mapping.forEach((key, value) -> copyed.mapping.put(key, value.deepCopy()));
        copyed.defaultRoleState = this.defaultRoleState;

        return copyed;
    }

    public enum DefaultRoleState {
        /**
         * No roles activated.
         */
        NONE((byte) 0),

        /**
         * All roles activated.
         */
        ALL((byte) 1),

        /**
         * Only Roles with default role flag activated.
         */
        ROLES((byte) 2);

        private static final Map<Byte, DefaultRoleState> STATE_ID_MAP = new HashMap<>(DefaultRoleState.values().length);

        static {
            for (DefaultRoleState state : DefaultRoleState.values()) {
                STATE_ID_MAP.put(state.getStateId(), state);
            }
        }

        /**
         * Used to store in db.
         */
        private final byte stateId;

        DefaultRoleState(byte stateId) {
            this.stateId = stateId;
        }

        public static DefaultRoleState lookupById(byte stateId) {
            return Optional.ofNullable(STATE_ID_MAP.get(stateId))
                .orElseThrow(() -> new IllegalArgumentException("Unrecognized state id: " + stateId));
        }

        public static DefaultRoleState from(MySqlSetDefaultRoleStatement.DefaultRoleSpec spec) {
            switch (spec) {
            case NONE:
                return NONE;
            case ALL:
                return ALL;
            case ROLES:
                return ROLES;
            default:
                throw new IllegalArgumentException("Unrecognized default role spec: " + spec);
            }
        }

        public byte getStateId() {
            return stateId;
        }
    }

    private static class GrantedRoleInfo {
        boolean withAdminOption = false;
        boolean defaultRole = false;

        public GrantedRoleInfo deepCopy() {
            GrantedRoleInfo info = new GrantedRoleInfo();
            info.withAdminOption = this.withAdminOption;
            info.defaultRole = this.defaultRole;

            return info;
        }
    }
}

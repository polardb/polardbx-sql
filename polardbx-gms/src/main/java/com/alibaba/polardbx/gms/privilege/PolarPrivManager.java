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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.privilege.PrivilegeUtil;
import com.alibaba.polardbx.common.utils.ExceptionUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.privilege.authorize.PolarAuthorizer;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_CHECK_PRIVILEGE_FAILED;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GRANTER_NO_GRANT_PRIV;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_OPERATION_NOT_ALLOWED;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;
import static com.alibaba.polardbx.common.privilege.UserPasswdChecker.MAX_USER_COUNT;
import static java.util.Collections.singletonList;

/**
 * @author shicai.xsc 2020/3/3 13:33
 * @since 5.0.0.0
 */
public class PolarPrivManager {

    private static final Logger logger = LoggerFactory.getLogger(PolarPrivManager.class);

    private static PolarPrivManager instance;
    private static MetaDbDataSource metaDbDataSource;
    private final PolarPrivilegeConfig config = new PolarPrivilegeConfig(this);
    /**
     * Mapping from account to account info.
     */
    private volatile PolarPrivilegeData accountPrivilegeData;
    private volatile Map<String, String> dbNameAppNameMap;
    private volatile Map<String, PolarLoginErr> loginErrMap = new HashMap<>();
    private volatile PolarLoginErrConfig polarLoginErrConfig;

    @VisibleForTesting
    public PolarPrivManager() {
        accountPrivilegeData = new PolarPrivilegeData(config.isRightsSeparationEnabled());
        dbNameAppNameMap = new HashMap<>();
    }

    public static PolarPrivManager getInstance() {
        if (instance == null) {
            synchronized (PolarPrivManager.class) {
                if (instance == null) {
                    instance = new PolarPrivManager();
                    instance.init();
                }
            }
        }
        return instance;
    }

    // region private methods
    private static void loadInstPriv(PolarPrivilegeData data, ResultSet rs)
        throws SQLException {
        while (rs.next()) {
            PolarAccountInfo userInfo = PolarInstPriv.loadInstPriv(rs);
            data.updateAccountData(userInfo);
        }
    }

    private static void loadDbPriv(PolarPrivilegeData data,
                                   ResultSet rs) throws SQLException {
        PolarDbPriv.loadDbPriv(rs, priv -> {
            PolarAccountInfo userInfo = data.getExactUser(priv.getUserName(), priv.getHost());

            if (userInfo != null) {
                userInfo.getDbPrivMap().put(priv.getIdentifier(), priv);
            }
        });
    }

    private PolarLoginErr selectLoginErr(String userName, String host) {
        if (userName == null) {
            return null;
        }
        if (loginErrMap == null) {
            return null;
        }
        String limitKey = userName.toUpperCase() + "@" + host;
        return loginErrMap.get(limitKey);

    }

    private List<PolarLoginErr> selectAllLoginErr() {
        return queryWithMetaDBConnection(conn -> {
            try {
                return MetaDbUtil.query(PolarPrivUtil.SELECT_ALL_LOGIN_ERROR_INFO, PolarLoginErr.class,
                    conn);
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query log for login error",
                    e.getMessage());
            }
        });
    }

    public void incrementLoginErrorCount(String userName, String host) {
        PolarLoginErrConfig polarLoginErrConfig = getPolarLoginErrConfig();
        int max = polarLoginErrConfig.getPasswordMaxErrorCount(userName);
        if (max <= 0) {
            return;
        }
        runWithMetaDBConnection(connection -> {
            try {
                String limitKey = userName.toUpperCase() + "@" + host;
                connection.setAutoCommit(false);
                incrementLoginErrorCount(userName, limitKey, connection);
                connection.commit();
            } catch (SQLException e) {
                logger.error(e.getMessage());
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                    //ignore
                }
            } finally {
                //after commit, we can reload it
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.LOGIN_ERROR_DATA_ID, null);
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.LOGIN_ERROR_DATA_ID);
            }
        });
    }

    private void incrementLoginErrorCount(String userName, String limitKey, Connection connection)
        throws SQLException {
        PolarLoginErrConfig polarLoginErrConfig = getPolarLoginErrConfig();
        int max = polarLoginErrConfig.getPasswordMaxErrorCount(userName);
        if (max <= 0) {
            return;
        }
        long expireSeconds = polarLoginErrConfig.getExpireSeconds(userName);
        //if cache do not contains it ,insert first and return
        if (!getLoginErrMap().containsKey(limitKey)) {
            try {
                insertLoginErr(limitKey, max, connection);
                return;
            } catch (SQLException e) {
                logger.error(e.getMessage());
                //maybe loss update, ignore it.
            }
        }
        PolarLoginErr polarLoginErr = getLoginErrMap().get(limitKey);
        AtomicBoolean enableUpdate = polarLoginErr.getEnableUpdate();
        //wait to get lock
        while (!enableUpdate.compareAndSet(true, false)) {
            waitMilliseconds();
        }
        //wait to listener
        while (polarLoginErr == null) {
            polarLoginErr = getLoginErrMap().get(limitKey);
            waitMilliseconds();
        }
        int errorCount = polarLoginErr.getErrorCount();
        int maxErrorLimit = polarLoginErr.getMaxErrorLimit();
        int oldErrorCount = polarLoginErr.getErrorCount();
        boolean needUpdate = false;
        if (maxErrorLimit != max) {
            //update
            needUpdate = true;
        }
        if (errorCount < max) {
            errorCount++;
            needUpdate = true;
        } else {
            if (polarLoginErr.getExpireDate().getTime() < System.currentTimeMillis()) {
                errorCount = 1;
                needUpdate = true;
            }
        }
        if (needUpdate) {
            long l = System.currentTimeMillis();
            if (errorCount == max) {
                l += expireSeconds * 1000;
            }
            if (logger.isInfoEnabled()) {
                logger.info("login error, and the limit key is " + limitKey + ", and the error count is " + errorCount);
            }
            Timestamp expireDate = new Timestamp(l);
            int i = updateLoginErr(limitKey, errorCount, max, expireDate, oldErrorCount, connection);
            polarLoginErr.setErrorCount(errorCount);
            polarLoginErr.setMaxErrorLimit(max);
            polarLoginErr.setExpireDate(expireDate);
            enableUpdate.compareAndSet(false, true);
            if (i <= 0) {
                incrementLoginErrorCount(userName, limitKey, connection);
            }
        } else {
            enableUpdate.compareAndSet(false, true);
        }
    }

    private static void waitMilliseconds() {
        try {
            TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException e) {
            Thread.interrupted();//ignore exception
        }
    }

    private static void insertLoginErr(String limitKey, int max, Connection connection) throws SQLException {
        Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
        MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setString, limitKey);
        MetaDbUtil.setParameter(2, insertParams, ParameterMethod.setInt, max);
        MetaDbUtil.setParameter(3, insertParams, ParameterMethod.setInt, 1);
        MetaDbUtil.insert(PolarPrivUtil.INSERT_LOGIN_ERROR_INFO, insertParams, connection);
    }

    private static int updateLoginErr(String limitKey, int errorCount, int max, Timestamp expireDate, int oldErrCount,
                                      Connection connection) throws SQLException {
        Map<Integer, ParameterContext> insertParams = Maps.newHashMap();
        MetaDbUtil.setParameter(1, insertParams, ParameterMethod.setInt, errorCount);
        MetaDbUtil.setParameter(2, insertParams, ParameterMethod.setTimestamp1, expireDate);
        MetaDbUtil.setParameter(3, insertParams, ParameterMethod.setInt, max);
        MetaDbUtil.setParameter(4, insertParams, ParameterMethod.setString, limitKey);
        MetaDbUtil.setParameter(5, insertParams, ParameterMethod.setInt, oldErrCount);
        // set error_count = ? ,expire_date = ?, max_error_limit = ? where limit_key = ? and error_count = ?
        return MetaDbUtil.update(PolarPrivUtil.UPDATE_LOGIN_ERROR_INFO, insertParams, connection);
    }

    public Map<String, String> getDbNameAppNameMap() {
        return this.dbNameAppNameMap;
    }

    public Map<String, PolarLoginErr> getLoginErrMap() {
        return this.loginErrMap;
    }

    public PolarLoginErrConfig getPolarLoginErrConfig() {
        return this.polarLoginErrConfig;
    }

    public void setPolarLoginErrConfig(PolarLoginErrConfig polarLoginErrConfig) {
        this.polarLoginErrConfig = polarLoginErrConfig;
    }

    protected boolean canEqual(final Object other) {
        return other instanceof PolarPrivManager;
    }

    @Override
    public String toString() {
        return "PolarPrivManager(userInfoMap=" + this.accountPrivilegeData + ", dbNameAppNameMap=" + this
            .getDbNameAppNameMap() + ", loginErrMap=" + this.getLoginErrMap() + ", polarLoginErrConfig=" + this
            .getPolarLoginErrConfig() + ", config=" + this.getConfig() + ")";
    }

    public void init() {
        if (metaDbDataSource == null) {
            metaDbDataSource = MetaDbDataSource.getInstance();
            reloadPriv();
            reloadLoginErr();
            registerConfigListener();
            registerLoginErrListener();
        }
    }

    public synchronized void reloadPriv() {
        Connection connection = null;
        Statement statement = null;
        try {
            try {
                connection = metaDbDataSource.getConnection();
                statement = connection.createStatement();

                ResultSet rs0 = statement.executeQuery(PolarPrivUtil.getSelectAllUserPrivSql());
                PolarPrivilegeData tmpUserInfoData = new PolarPrivilegeData(config.isRightsSeparationEnabled());
                loadInstPriv(tmpUserInfoData, rs0);

                ResultSet rs1 = statement.executeQuery(PolarPrivUtil.getSelectAllDbPrivSql());
                loadDbPriv(tmpUserInfoData, rs1);

                ResultSet rs2 = statement.executeQuery(PolarPrivUtil.getSelectAllTablePrivSql());
                loadTbPriv(tmpUserInfoData, rs2);

                PolarRolePrivilege.loadFromDb(connection, tmpUserInfoData);

                accountPrivilegeData = tmpUserInfoData;

                reLoadAllDbs();
            } finally {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        } catch (Throwable e) {
            logger.error("Failed to reload privilege data", e);
        }
    }

    public synchronized void reloadLoginErr() {
        runWithMetaDBConnection(connection -> {
            Statement statement = null;
            try {
                try {
                    statement = connection.createStatement();
                    List<PolarLoginErr> polarLoginErrList = selectAllLoginErr();
                    Map<String, PolarLoginErr> map = new HashedMap();
                    if (polarLoginErrList != null) {
                        polarLoginErrList.stream().forEach(t -> map.put(t.getLimitKey().toUpperCase(), t));
                    }
                    this.loginErrMap = map;
                } finally {
                    if (statement != null) {
                        statement.close();
                    }
                }
            } catch (Throwable e) {
                logger.error("Failed to reload privilege data", e);
            }
        });
    }

    public Set<String> getAllDbs() {
        return this.dbNameAppNameMap.keySet();
    }

    public Map<String, String> getAllDbNameAndAppNameMap() {
        return this.dbNameAppNameMap;
    }

    // endregion

    public Set<String> getUsageDbs(String userName, String host, ActiveRoles activeRoles) {
        Set<String> allDbs = getAllDbs();

        PolarAccountInfo userInfo = accountPrivilegeData.getMatchUser(userName, host);

        Set<String> ret = new HashSet<>(allDbs.size());
        // Every one has permission on information schema
        ret.add(PolarPrivUtil.INFORMATION_SCHEMA);
        for (String db : allDbs) {
            PermissionCheckContext context = new PermissionCheckContext(userInfo.getAccountId(), activeRoles,
                Permission.anyPermission(db, ""));
            if (checkPermission(context)) {
                ret.add(db);
            }
        }

        return ret;
    }

    /**
     * Show grants handler.
     *
     * @param currentUser The user who wants to show current user.
     * @param activeRoles Current user's active roles.
     * @param forUser Privilege's owner to show.
     * @param roles Using roles.
     * @return Grants, one line per element(global, db, table)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/show-grants.html">Show Grants</a>
     */
    public List<String> showGrants(PolarAccountInfo currentUser, ActiveRoles activeRoles, PolarAccount forUser,
                                   List<PolarAccount> roles) {
        Preconditions.checkNotNull(currentUser, "Current user can't be null!");
        Preconditions.checkNotNull(forUser, "Account can't be null!");
        Preconditions.checkNotNull(roles, "Roles can't be null!");

        PolarAccountInfo currentUserInfo = accountPrivilegeData.getAndCheckExactUser(currentUser.getAccount());
        PolarAccountInfo forUserInfo = accountPrivilegeData.getAndCheckExactUser(forUser);

        if (currentUserInfo.getAccountId() != forUserInfo.getAccountId()) {
            PermissionCheckContext permissionCheckContext = new PermissionCheckContext(
                currentUserInfo.getAccountId(),
                activeRoles,
                Permission.databasePermission(PolarPrivUtil.INFORMATION_SCHEMA, PrivilegeKind.SELECT)
            );
            if (!checkPermission(permissionCheckContext)) {
                throw new TddlRuntimeException(ERR_CHECK_PRIVILEGE_FAILED, "Show Grants",
                    currentUserInfo.getUsername(), currentUserInfo.getHost());
            }
        }

        List<PolarAccountInfo> roleInfoList = roles.stream()
            .map(accountPrivilegeData::getAndCheckExactUser)
            .collect(Collectors.toList());

        roleInfoList.forEach(r -> forUserInfo.getRolePrivileges().checkRoleGranted(r.getAccount()));

        PolarAccountInfo newAccountInfo = forUserInfo.deepCopy();

        if (currentUserInfo.getAccountId() == forUserInfo.getAccountId()) {
            newAccountInfo.getRolePrivileges().getActiveRoleIds(activeRoles, config.getMandatoryRoleIds())
                .stream()
                .map(accountPrivilegeData::getById)
                .filter(Objects::nonNull)
                .forEach(newAccountInfo::mergeUserPrivileges);
        }

        roleInfoList.forEach(newAccountInfo::mergeUserPrivileges);
        return newAccountInfo.showGrants(accountPrivilegeData);
    }

    /**
     * Create an account.
     * <p>
     * This method checks whether {@code granter} has permissions to create user. And it requires that all {@code
     * grantees} have same {@link AccountType}.
     *
     * @param granter Account creator.
     * @param grantees Accounts to be created. They must all have same {@link AccountType}.
     * @return Creation result.
     * @see AccountType
     */
    public void createAccount(PolarAccountInfo granter, ActiveRoles activeRoles, List<PolarAccountInfo> grantees,
                              boolean ifNotExists) {
        Preconditions.checkNotNull(granter, "Creator can't be null!");
        Preconditions.checkArgument(!Iterables.isEmpty(grantees), "Accounts list can't be empty!");
        Preconditions.checkArgument(grantees.stream()
                .map(PolarAccountInfo::getAccountType)
                .distinct()
                .count() == 1,
            "All accounts must have same account type!");

        // Check permission
        checkModifyReservedAccounts(granter, grantees, false);
        {
            PermissionCheckContext context = new PermissionCheckContext(granter.getAccountId(), activeRoles,
                Permission.instancePermission(PrivilegeKind.CREATE_USER));
            if (!checkPermission(context)) {
                throw new TddlRuntimeException(ERR_CHECK_PRIVILEGE_FAILED, "CREATE ACCOUNT", granter.getUsername(),
                    granter.getHost());
            }
        }

        synchronized (this) {
            if ((accountPrivilegeData.getAccountCount() + grantees.size()) > MAX_USER_COUNT) {
                throw new TddlRuntimeException(ErrorCode.ERR_ACCOUNT_LIMIT_EXCEEDED);
            }
        }

        runWithMetaDBConnection(conn -> {
            try {
                conn.setAutoCommit(false);
                logger.info("Start create accounts.");
                try (Statement stat = conn.createStatement()) {
                    for (PolarAccountInfo newUser : grantees) {
                        stat.executeUpdate(PolarPrivUtil.getInsertUserPrivSql(newUser, ifNotExists));
                        // Grant information schema privilege to new user.
                        PolarDbPriv informationSchemaPrivilege =
                            PolarDbPriv.toInformationSchemaPriv(newUser.getAccount());
                        stat.executeUpdate(informationSchemaPrivilege.toInsertNewSql());
                        Optional<String> sqlOpt = informationSchemaPrivilege.toUpdatePrivilegeSql(true);
                        if (sqlOpt.isPresent()) {
                            stat.executeUpdate(sqlOpt.get());
                        }
                    }
                }
                conn.commit();
                logger.info("Accounts created.");

                reloadAccounts(conn, grantees
                    .stream()
                    .map(PolarAccountInfo::getAccount)
                    .collect(Collectors.toList()));
            } catch (Throwable t) {
                logger.error("Failed to create accounts!", t);
                if (ExceptionUtils.isMySQLIntegrityConstraintViolationException(t)) {
                    if (t.getMessage() != null && t.getMessage().contains("Duplicate entry")) {
                        throw new TddlRuntimeException(ERR_SERVER, "Failed to create account due to duplicate entry!",
                            t);
                    }
                }
                throw new TddlRuntimeException(ERR_SERVER, "Failed to persist account data!", t);
            }
        });

        triggerReload();
    }

    /**
     * Drop an account.
     * <p>
     * This method checks whether {@code granter} has permissions to drop user. And it requires that all {@code
     * grantees} have same {@link AccountType}.
     *
     * @param granter Account creator.
     * @param grantees Accounts to be created. They must all have same {@link AccountType}.
     * @param ifExists If false, will verify all grantees exists, otherwise ignore nonexisting accounts.
     * @see AccountType
     */
    public void dropAccount(PolarAccountInfo granter, ActiveRoles activeRoles, List<PolarAccountInfo> grantees,
                            boolean ifExists) {
        Preconditions.checkNotNull(granter, "Creator can't be null!");
        Preconditions.checkArgument(CollectionUtils.isNotEmpty(grantees), "Accounts list can't be empty!");
        Preconditions.checkArgument(grantees.stream()
                .map(PolarAccountInfo::getAccountType)
                .distinct()
                .count() == 1,
            "All accounts must have same account type!");

        {
            checkModifyReservedAccounts(granter, grantees, true);
            PermissionCheckContext context = new PermissionCheckContext(granter.getAccountId(), activeRoles,
                Permission.instancePermission(PrivilegeKind.CREATE_USER));
            if (!checkPermission(context)) {
                throw new TddlRuntimeException(ERR_CHECK_PRIVILEGE_FAILED, "CREATE ACCOUNT", granter.getUsername(),
                    granter.getHost());
            }
        }

        runWithMetaDBConnection(conn -> {
            try {
                List<Long> accountIds = grantees.stream()
                    .map(PolarAccountInfo::getAccount)
                    .map(acc -> accountPrivilegeData.getExactUser(acc, !ifExists))
                    .filter(Objects::nonNull)
                    .map(PolarAccountInfo::getAccountId)
                    .collect(Collectors.toList());

                // Delete data from database.
                conn.setAutoCommit(false);
                logger.info("Starting to drop users.");
                try (Statement stmt = conn.createStatement()) {
                    for (PolarAccountInfo user : grantees) {
                        stmt.executeUpdate(PolarPrivUtil.getDeleteUserPrivSql(user));
                        stmt.executeUpdate(PolarPrivUtil.getDeleteAllDbPrivSql(user));
                        stmt.executeUpdate(PolarPrivUtil.getDeleteAllTablePrivSql(user));
                    }
                }

                PolarRolePrivilege.dropAccounts(conn, accountIds);

                conn.commit();
                logger.info("Finished deleting account data!");
            } catch (SQLException e) {
                logger.error("Failed to drop accounts.", e);
                throw new TddlRuntimeException(ERR_SERVER, "Failed to persist account data!");
            }
        });

        reloadPriv();
        triggerReload();
    }

    public void grantPrivileges(PolarAccountInfo granter, ActiveRoles activeRoles, List<PolarAccountInfo> grantees) {
        checkModifyReservedAccounts(granter, grantees, false);

        // check granter privilege
        // If has create user permission, it should be able to grant privileges to all
        boolean hasCreateUserPermission = checkPermission(new PermissionCheckContext(granter.getAccountId(),
            activeRoles, Permission.instancePermission(PrivilegeKind.CREATE_USER)));

        if (!hasCreateUserPermission) {
            PolarAccountInfo samplePermission = grantees.get(0).deepCopy();
            samplePermission.addGrantOptionToAll();

            for (Permission permission : samplePermission.toPermissions()) {
                PermissionCheckContext context = new PermissionCheckContext(granter.getAccountId(), activeRoles,
                    permission);
                if (!checkPermission(context)) {
                    throw new TddlRuntimeException(ERR_GRANTER_NO_GRANT_PRIV, granter.getIdentifier(),
                        samplePermission.getIdentifier());
                }
            }
        }

        for (PolarAccountInfo user : grantees) {
            if (!accountPrivilegeData.containsUser(user.getAccount())) {
                throw new TddlRuntimeException(ErrorCode.ERR_USER_NOT_EXISTS, user.getIdentifier());
            }
        }

        runWithMetaDBConnection(conn -> {
            try {
                conn.setAutoCommit(false);
                logger.info("Start to persist privilege data.");
                try (Statement stat = conn.createStatement()) {
                    for (PolarAccountInfo user : grantees) {
                        for (String sql : user.toUpdatePrivilegeSqls(true)) {
                            stat.executeUpdate(sql);
                        }
                    }
                }
                conn.commit();

                logger.info("Finished persisting privilege data.");
                reloadAccounts(conn, grantees.stream().map(PolarAccountInfo::getAccount).collect(Collectors.toList()));
            } catch (Throwable t) {
                logger.error("Failed to persist privilege data.", t);
                throw new TddlRuntimeException(ERR_SERVER, "Failed to sync privileges to metadb.", t);
            }
        });

        triggerReload();
    }

    public void revokePrivileges(PolarAccountInfo granter, ActiveRoles activeRoles, List<PolarAccountInfo> grantees) {
        checkModifyReservedAccounts(granter, grantees, false);
        // check granter privilege
        // If has create user permission, it should be able to grant privileges to all
        boolean hasCreateUserPermission = checkPermission(new PermissionCheckContext(granter.getAccountId(),
            activeRoles, Permission.instancePermission(PrivilegeKind.CREATE_USER)));

        if (!hasCreateUserPermission) {
            PolarAccountInfo samplePermission = grantees.get(0).deepCopy();
            samplePermission.addGrantOptionToAll();

            for (Permission permission : samplePermission.toPermissions()) {
                PermissionCheckContext context = new PermissionCheckContext(granter.getAccountId(), activeRoles,
                    permission);
                if (!checkPermission(context)) {
                    throw new TddlRuntimeException(ERR_GRANTER_NO_GRANT_PRIV, granter.getIdentifier(),
                        samplePermission.getIdentifier());
                }
            }
        }

        runWithMetaDBConnection(conn -> {
            try {
                conn.setAutoCommit(false);
                logger.info("Start to persist privilege data.");
                try (Statement stat = conn.createStatement()) {
                    for (PolarAccountInfo user : grantees) {
                        for (String sql : user.toUpdatePrivilegeSqls(false)) {
                            stat.executeUpdate(sql);
                        }
                    }
                }
                conn.commit();

                logger.info("Finished persisting privilege data.");
                reloadAccounts(conn, grantees.stream().map(PolarAccountInfo::getAccount).collect(Collectors.toList()));
            } catch (Throwable t) {
                logger.error("Failed to persist privilege data.", t);
                throw new TddlRuntimeException(ERR_SERVER, "Failed to sync privileges to metadb.", t);
            }
        });

        triggerReload();
    }

    public void setPassword(PolarAccountInfo granter, ActiveRoles activeRoles, PolarAccountInfo grantee) {
        PolarAccountInfo currentUser = getAndCheckExactUser(granter.getAccount());
        PolarAccountInfo userToChange = getAndCheckExactUser(grantee.getAccount());
        // Copy password
        userToChange.setPassword(grantee.getPassword());

        if (currentUser.getAccountId() != userToChange.getAccountId()) {
            // For reserved accounts, only self can change password.
            checkModifyReservedAccounts(granter, singletonList(grantee), true);
            PermissionCheckContext context = new PermissionCheckContext(currentUser.getAccountId(), activeRoles,
                Permission.instancePermission(PrivilegeKind.CREATE_USER));

            if (!checkPermission(context)) {
                throw new TddlRuntimeException(ERR_CHECK_PRIVILEGE_FAILED, PrivilegeKind.CREATE_USER.name(),
                    currentUser.getUsername(), currentUser.getHost());
            }
        }

        AccountType accountType = userToChange.getAccount().getAccountType();
        if (accountType == AccountType.ROLE) {
            throw new TddlRuntimeException(ERR_OPERATION_NOT_ALLOWED, "Set password for role.");
        }

        runWithMetaDBConnection(conn -> {
            logger.info("Starting to set password for user: " + userToChange.getAccount().getIdentifier());
            try {
                String sql = PolarPrivUtil.getUpdateUserPrivSql(userToChange, PrivManageType.SET_PASSWORD);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(sql);
                }
                conn.commit();
                reloadAccounts(conn, singletonList(userToChange.getAccount()));
                logger.info("Succeeded to set password for " + userToChange.getAccount().getIdentifier());
            } catch (Exception e) {
                logger.error("Failed to set password for " + userToChange.getAccount().getIdentifier(), e);
                throw new TddlRuntimeException(ERR_SERVER, "Failed to persist account data.", e);
            }
        });

        triggerReload();
    }

    private void loadTbPriv(PolarPrivilegeData data,
                            ResultSet rs) throws SQLException {
        PolarTbPriv.loadTbPriv(rs, priv -> {
            PolarAccountInfo userInfo = data.getExactUser(priv.getUserName(), priv.getHost());
            if (userInfo != null) {
                userInfo.getTbPrivMap().put(priv.getIdentifier(), priv);
            }
        });
    }

    private Set<String> reLoadAllDbs() {

        Map<String, String> allDbs = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Connection connection = null;
        Statement statement = null;
        String sql = PolarPrivUtil.getLoadAllDbsSql();
        try {
            try {
                connection = metaDbDataSource.getConnection();
                statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql);
                while (resultSet.next()) {
                    String dbName = resultSet.getString(1);
                    String appName = resultSet.getString(2);
                    allDbs.put(dbName, appName);
                }
                this.dbNameAppNameMap = allDbs;
                return this.dbNameAppNameMap.keySet();
            } finally {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            }
        } catch (Throwable e) {
            logger.error(String.format("Failed in loadAllDbs, sql: %s", sql), e);
            throw GeneralUtil.nestedException(e);
        }

    }

    public String encryptPassword(String password) {
        return PrivilegeUtil.encryptPassword(password).getPassword();
    }

    public boolean checkUserLoginErrMaxCount(String userName, String host) {
        if (userName == null) {
            return true;
        }
        PolarLoginErr polarLoginErr = selectLoginErr(userName, host);
        if (polarLoginErr == null) {
            return true;
        }
        PolarLoginErrConfig polarLoginErrConfig = getPolarLoginErrConfig();
        int max = polarLoginErrConfig.getPasswordMaxErrorCount(userName);
        if (max <= 0) {
            return true;
        }
        int count = polarLoginErr.getErrorCount();
        return count < max || polarLoginErr.getExpireDate().getTime() < System.currentTimeMillis();
    }

    public boolean checkUserPasswordExpire(String userName, String host) {
        if (userName == null) {
            return true;
        }
        PolarLoginErrConfig polarLoginErrConfig = getPolarLoginErrConfig();
        Date expireDate = polarLoginErrConfig.getPasswordExpireDate(userName);
        if (expireDate == null) {
            return true;
        }
        Date now = new Date();
        if (now.after(expireDate)) {
            return false;
        }
        return true;
    }

    private void registerConfigListener() {
        MetaDbConfigManager.getInstance()
            .register(MetaDbDataIdBuilder.getPrivilegeInfoDataId(), null);
        MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getPrivilegeInfoDataId(),
            new PrivilegeInfoConfigListener());
    }

    public void triggerReload() {
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getPrivilegeInfoDataId(), null);
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getPrivilegeInfoDataId());
    }

    private void registerLoginErrListener() {
        MetaDbConfigManager.getInstance()
            .register(MetaDbDataIdBuilder.LOGIN_ERROR_DATA_ID, null);
        MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.LOGIN_ERROR_DATA_ID,
            new UserLoginErrorChangeListener());
    }

    public void runWithMetaDBConnection(Consumer<Connection> consumer) {
        try (Connection conn = metaDbDataSource.getConnection()) {
            conn.setAutoCommit(false);
            consumer.accept(conn);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ERR_SERVER, e, "Metadb error!");
        }
    }

    public <R> R queryWithMetaDBConnection(Function<Connection, R> query) {
        try (Connection conn = metaDbDataSource.getConnection()) {
            conn.setAutoCommit(false);
            return query.apply(conn);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ERR_SERVER, e, "Metadb error!");
        }
    }

    public PolarPrivilegeConfig getConfig() {
        return config;
    }
    // endregion

    /**
     * Return current role of account.
     *
     * @param accountId Account id.
     * @param activeRoles Nullable active role settings for current session.. See {@link ActiveRoles}
     * @return Role accounts.
     */
    public List<PolarAccount> getCurrentRole(long accountId, ActiveRoles activeRoles) {
        PolarAccountInfo current = accountPrivilegeData.getAndCheckById(accountId);

        Collection<Long> roleIds;
        if (activeRoles != null) {
            roleIds = current.getRolePrivileges().getActiveRoleIds(activeRoles, config.getMandatoryRoleIds());
        } else if (config.isActiveAllRolesOnLogin()) {
            roleIds = current.getRolePrivileges().getAllRoles(config.getMandatoryRoleIds());
        } else {
            roleIds = current.getRolePrivileges().getDefaultRoles(config.getMandatoryRoleIds());
        }

        return new HashSet<>(roleIds).stream()
            .map(accountPrivilegeData::getAndCheckById)
            .map(PolarAccountInfo::getAccount)
            .collect(Collectors.toList());
    }

    public void reloadAccounts(Connection conn, Collection<PolarAccount> accounts)
        throws SQLException {
        logger.info("Start to reload accounts.");
        synchronized (this) {
            accountPrivilegeData.updateAccountData(PolarAccountInfo.loadPolarAccounts(conn, accounts));
        }
        logger.info("Finished reloading accounts.");
    }

    public PolarAccountInfo getExactUserByIdentifier(String identifier) {
        return accountPrivilegeData.getExactUser(PolarAccount.fromIdentifier(identifier));
    }

    public PolarAccountInfo getAndCheckById(long accountId) {
        return accountPrivilegeData.getAndCheckById(accountId);
    }

    public PolarAccountInfo getAndCheckExactUser(PolarAccount account) {
        return accountPrivilegeData.getAndCheckExactUser(account);
    }

    public PolarAccountInfo getAndCheckExactUser(String username, String host) {
        return accountPrivilegeData.getAndCheckExactUser(username, host);
    }

    public PolarAccountInfo getExactUser(String username, String host) {
        return accountPrivilegeData.getExactUser(username, host);
    }

    public PolarAccountInfo getMatchUser(String username, String host) {
        return accountPrivilegeData.getMatchUser(username, host);
    }

    /**
     * Check whether user has required permission. It checks user's privileges first. If requirements not met, then
     * check whether any active role meets.
     *
     * @param context Input for this check.
     * @return Whether user and its active roles meets permission requirements.
     * @see PermissionCheckContext
     * @see PolarAuthorizer#hasPermission
     */
    public boolean checkPermission(PermissionCheckContext context) {
        PolarAccountInfo account = accountPrivilegeData.getAndCheckById(context.getAccountId());
        if (PolarAuthorizer.hasPermission(account, context.getPermission())) {
            return true;
        }

        Collection<Long> activeRoleIds =
            account.getRolePrivileges().getActiveRoleIds(context.getActiveRoles(), config.getMandatoryRoleIds());
        for (long roleId : activeRoleIds) {
            PolarAccountInfo role = accountPrivilegeData.getById(roleId);
            if (role != null && PolarAuthorizer.hasPermission(role, context.getPermission())) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    public PolarPrivilegeData getAccountPrivilegeData() {
        return accountPrivilegeData;
    }

    @VisibleForTesting
    public void setAccountPrivilegeData(PolarPrivilegeData accountPrivilegeData) {
        this.accountPrivilegeData = accountPrivilegeData;
    }

    protected static class PrivilegeInfoConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            logger.info(String.format("start reload privilege config, newOpVersion: %d", newOpVersion));
            PolarPrivManager.getInstance().reloadPriv();
            logger.info("finish reload privilege config");
        }
    }

    protected static class UserLoginErrorChangeListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            logger.info(String.format("start reload login config, newOpVersion: %d", newOpVersion));
            PolarPrivManager.getInstance().reloadLoginErr();
            logger.info("finish reload privilege config");
        }
    }

    private void checkModifyReservedAccount(PolarAccountInfo user, PolarAccount account, boolean godAllowed) {
        if (accountPrivilegeData.isReservedRole(account)) {
            throw new TddlRuntimeException(ERR_OPERATION_NOT_ALLOWED, "Modifying reserved role "
                + account.getIdentifier() + " is not allowed!");
        }

        if (accountPrivilegeData.isReservedUser(account)) {
            if (!godAllowed || !user.getAccount().getAccountType().isGod()) {
                throw new TddlRuntimeException(ERR_OPERATION_NOT_ALLOWED, "Modifying reserved user "
                    + account.getIdentifier() + " is not allowed!");
            }
        }
    }

    /**
     * Test whether reserved account can be changed. This used to protect reserved accounts in rights separation mode.
     *
     * @param user User who wants to change account.
     * @param accounts Accounts to be change.
     * @param godAllowed Whether god can execute this modification.
     */
    public void checkModifyReservedAccounts(PolarAccountInfo user, Collection<PolarAccountInfo> accounts,
                                            boolean godAllowed) {
        Preconditions.checkNotNull(user, "User can't be null!");
        Preconditions.checkState(!Iterables.isEmpty(accounts), "Accounts can't be empty!");
        PolarAccountInfo realUser = accountPrivilegeData.getAndCheckById(user.getAccountId());
        accounts.forEach(account -> checkModifyReservedAccount(realUser, account.getAccount(), godAllowed));
    }

    public List<Object[]> listUserPrivileges(PolarAccountInfo currentAccount) {
        List<Object[]> ret = new ArrayList<>();
        boolean isGod = currentAccount.getAccountType() == AccountType.GOD;
        accountPrivilegeData.consumeAccountInfos(user -> {
            if (!isGod && !StringUtils.equalsIgnoreCase(currentAccount.getUsername(), user.getUsername())) {
                // 只有高权限账号才能查看所有用户的权限
                return;
            }
            String identifier = user.getIdentifier();
            String isGrantable = user.getInstPriv().hasPrivilege(PrivilegeKind.GRANT_OPTION) ? "YES" : "NO";
            user.getInstPriv().listGrantedPrivileges()
                .stream()
                .map(privilege -> new Object[] {identifier, "def", privilege, isGrantable})
                .forEach(ret::add);
        });
        return ret;
    }

    /**
     * GRANTEE:
     * TABLE_CATALOG: def
     * TABLE_SCHEMA:
     * TABLE_NAME:
     * PRIVILEGE_TYPE:
     * IS_GRANTABLE:
     */
    public List<Object[]> listTablePrivileges(PolarAccountInfo currentAccount) {
        List<Object[]> ret = new ArrayList<>();
        boolean isGod = currentAccount.getAccountType() == AccountType.GOD;
        accountPrivilegeData.consumeAccountInfos(user -> {
            if (!isGod && !StringUtils.equalsIgnoreCase(currentAccount.getUsername(), user.getUsername())) {
                // 只有高权限账号才能查看所有用户的table权限
                return;
            }
            String identifier = user.getIdentifier();
            String isGrantable = user.getInstPriv().hasPrivilege(PrivilegeKind.GRANT_OPTION) ? "YES" : "NO";
            user.getTbPrivMap().values()
                .forEach(tbPriv -> tbPriv.getPrivileges().entrySet()
                    .stream()
                    .filter(priv -> priv.getKey().hasSqlName())
                    .filter(Map.Entry::getValue)
                    .map(priv -> new Object[] {
                        identifier, "def", tbPriv.getDbName(), tbPriv.getTbName(),
                        priv.getKey().getSqlName(), isGrantable})
                    .forEach(ret::add));
        });
        return ret;
    }

    /**
     * GRANTEE:
     * TABLE_CATALOG: def
     * TABLE_SCHEMA:
     * PRIVILEGE_TYPE:
     * IS_GRANTABLE:
     */
    public List<Object[]> listSchemaPrivileges(PolarAccountInfo currentAccount) {
        List<Object[]> ret = new ArrayList<>();
        boolean isGod = currentAccount.getAccountType() == AccountType.GOD;
        accountPrivilegeData.consumeAccountInfos(user -> {
            if (!isGod && !StringUtils.equalsIgnoreCase(currentAccount.getUsername(), user.getUsername())) {
                // 只有高权限账号才能查看所有用户的schema权限
                return;
            }
            String identifier = user.getIdentifier();
            String isGrantable = user.getInstPriv().hasPrivilege(PrivilegeKind.GRANT_OPTION) ? "YES" : "NO";
            user.getDbPrivMap().entrySet()
                .stream()
                .filter(entry -> !StringUtils.equalsIgnoreCase(entry.getKey(), PolarPrivUtil.INFORMATION_SCHEMA))
                .forEach(entry -> entry.getValue().getPrivileges().entrySet()
                    .stream()
                    .filter(priv -> priv.getKey().hasSqlName())
                    .filter(Map.Entry::getValue)
                    .map(priv -> new Object[] {
                        identifier, "def", entry.getKey(), priv.getKey().getSqlName(), isGrantable})
                    .forEach(ret::add));
        });
        return ret;
    }

    public synchronized void changeRightsSeparationMode(boolean enabled) {
        accountPrivilegeData.changeRightsSeparationMode(enabled);
    }
}

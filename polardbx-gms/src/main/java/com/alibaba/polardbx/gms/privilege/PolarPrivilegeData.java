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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER;
import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_USER_NOT_EXISTS;
import static com.alibaba.polardbx.common.privilege.UserPasswdChecker.MAX_USER_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * @author bairui.lrj
 * @since 5.4.9
 */
public class PolarPrivilegeData {
    private final ConcurrentMap<Long, PolarAccountInfo> userInfoMap = new ConcurrentHashMap<>(MAX_USER_COUNT);
    private final ConcurrentMap<PolarAccount, Long> accountToIdMap = new ConcurrentHashMap<>(MAX_USER_COUNT);
    /**
     * To keep three reserved account: DBA, SSO, AUDITOR
     *
     * @see AccountType
     */
    private final ConcurrentMap<AccountType, PolarAccount> reservedUsers = new ConcurrentHashMap<>(3);

    private boolean rightsSeparationEnabled;

    public PolarPrivilegeData() {
        this(false);
    }

    public PolarPrivilegeData(boolean rightsSeparationEnabled) {
        this.rightsSeparationEnabled = rightsSeparationEnabled;
        updateAccountData(asList(PolarReservedAccounts.ROLE_DBA_ACCOUNT, PolarReservedAccounts.ROLE_ADMIN_ACCOUNT, PolarReservedAccounts.ROLE_SECURITY_OFFICER_ACCOUNT,
            PolarReservedAccounts.ROLE_AUDITOR_ACCOUNT));
    }

    private static PolarAccountInfo newDBAPolarAccountInfo(PolarAccountInfo value, boolean rightsSeparationEnabled) {
        PolarAccountInfo newAccountInfo = value.deepCopy();
        newAccountInfo.clearAllPrivileges();
        if (rightsSeparationEnabled) {
            newAccountInfo.getRolePrivileges()
                .replaceRoles(singletonList(PolarReservedAccounts.ROLE_DBA_ACCOUNT));
        } else {
            newAccountInfo.getRolePrivileges()
                .replaceRoles(asList(PolarReservedAccounts.ROLE_DBA_ACCOUNT, PolarReservedAccounts.ROLE_ADMIN_ACCOUNT, PolarReservedAccounts.ROLE_SECURITY_OFFICER_ACCOUNT,
                    PolarReservedAccounts.ROLE_AUDITOR_ACCOUNT));
        }
        newAccountInfo.getRolePrivileges().updateDefaultRole(PolarRolePrivilege.DefaultRoleState.ALL, emptyList());
        return newAccountInfo;
    }

    private static PolarAccountInfo newSSOAccountInfo(PolarAccountInfo value, boolean rightsSeparationEnabled) {
        PolarAccountInfo newAccountInfo = value.deepCopy();
        newAccountInfo.clearAllPrivileges();
        if (rightsSeparationEnabled) {
            newAccountInfo.getRolePrivileges()
                .replaceRoles(singletonList(PolarReservedAccounts.ROLE_SECURITY_OFFICER_ACCOUNT));
        }
        newAccountInfo.getRolePrivileges().updateDefaultRole(PolarRolePrivilege.DefaultRoleState.ALL, emptyList());
        return newAccountInfo;
    }

    private static PolarAccountInfo newAuditorAccountInfo(PolarAccountInfo value, boolean rightsSeparationEnabled) {
        PolarAccountInfo newAccountInfo = value.deepCopy();
        newAccountInfo.clearAllPrivileges();
        if (rightsSeparationEnabled) {
            newAccountInfo.getRolePrivileges()
                .replaceRoles(singletonList(PolarReservedAccounts.ROLE_AUDITOR_ACCOUNT));
        }
        newAccountInfo.getRolePrivileges().updateDefaultRole(PolarRolePrivilege.DefaultRoleState.ALL, emptyList());
        return newAccountInfo;
    }

    private static PolarAccountInfo newGodAccountInfo(PolarAccountInfo value) {
        PolarAccountInfo newAccountInfo = value.deepCopy();
        newAccountInfo.getRolePrivileges()
            .replaceRoles(asList(PolarReservedAccounts.ROLE_DBA_ACCOUNT, PolarReservedAccounts.ROLE_ADMIN_ACCOUNT, PolarReservedAccounts.ROLE_SECURITY_OFFICER_ACCOUNT,
                PolarReservedAccounts.ROLE_AUDITOR_ACCOUNT));
        newAccountInfo.getRolePrivileges().updateDefaultRole(PolarRolePrivilege.DefaultRoleState.ALL, emptyList());
        return newAccountInfo;
    }

    public void updateAccountData(Collection<PolarAccountInfo> data) {
        data.forEach(this::updateAccountData);
    }

    public int getAccountCount() {
        return userInfoMap.size();
    }

    public void updateAccountData(PolarAccountInfo value) {
        if (value.getAccountType().isReservedUser()) {
            updateReservedAccount(value);
        } else {
            updateAccountDataUnchecked(value);
        }
    }

    private synchronized void updateReservedAccount(PolarAccountInfo value) {
        PolarAccountInfo newAccountInfo;
        switch (value.getAccountType()) {
        case DBA:
            newAccountInfo = newDBAPolarAccountInfo(value, rightsSeparationEnabled);
            break;
        case SSO:
            newAccountInfo = newSSOAccountInfo(value, rightsSeparationEnabled);
            break;
        case AUDITOR:
            newAccountInfo = newAuditorAccountInfo(value, rightsSeparationEnabled);
            break;
        case GOD:
            newAccountInfo = newGodAccountInfo(value);
            break;
        default:
            throw new IllegalArgumentException("Unrecognized reserved account type:" + value.getAccountType());
        }

        updateAccountDataUnchecked(newAccountInfo);
    }

    private void updateAccountDataUnchecked(PolarAccountInfo value) {
        userInfoMap.put(value.getAccountId(), value);
        accountToIdMap.put(value.getAccount(), value.getAccountId());
        if (value.getAccount().getAccountType().isReservedUser()) {
            reservedUsers.put(value.getAccount().getAccountType(), value.getAccount());
        }
    }

    public PolarAccountInfo getExactUser(String username, String host) {
        return getExactUser(PolarAccount.newBuilder().setUsername(username).setHost(host).build());
    }

    public PolarAccountInfo getExactUser(PolarAccount account) {
        return Optional.ofNullable(accountToIdMap.get(account))
            .flatMap(id -> Optional.ofNullable(userInfoMap.get(id)))
            .orElse(null);
    }

    public PolarAccountInfo getExactUser(PolarAccount account, boolean check) {
        if (check) {
            return getAndCheckExactUser(account);
        } else {
            return getExactUser(account);
        }
    }

    public boolean containsUser(PolarAccount account) {
        return getExactUser(account) != null;
    }

    public PolarAccountInfo getById(long accountId) {
        return userInfoMap.get(accountId);
    }

    public PolarAccountInfo getAndCheckById(long accountId) {
        return Optional.ofNullable(getById(accountId))
            .orElseThrow(() -> new IllegalArgumentException("Account id not found: " + accountId));
    }

    public PolarAccountInfo getMatchUser(String userName, String host) {
        PolarAccountInfo res = null;
        int maxCommonPrefix = -1;

        for (PolarAccountInfo userInfo : userInfoMap.values()) {
            if (userInfo.isMatch(userName, host)) {
                int commonPrefix = PolarPrivUtil.getCommonPrefixLength(userInfo.getHost(), host);
                if (commonPrefix > maxCommonPrefix) {
                    maxCommonPrefix = commonPrefix;
                    res = userInfo;
                }
            }
        }
        return res;
    }

    /**
     * Match account by exact username and host. Throws exception when not exsits.
     *
     * @param input Containing username and host.
     * @return Polar account matches.
     */
    public PolarAccountInfo getAndCheckExactUser(PolarAccount input) {
        PolarAccountInfo accountInfo = getExactUser(input);
        if (accountInfo == null) {
            throw new TddlRuntimeException(ERR_USER_NOT_EXISTS, input.getIdentifier());
        }

        return accountInfo;
    }

    public PolarAccountInfo getAndCheckExactUser(String username, String host) {
        return getAndCheckExactUser(PolarAccount.newBuilder().setUsername(username).setHost(host).build());
    }

    private PolarAccountInfo getReservedUser(AccountType accountType) {
        return Optional.ofNullable(reservedUsers.get(accountType))
            .map(this::getExactUser)
            .orElse(null);
    }

    private PolarAccountInfo getAndCheckReservedUser(AccountType accountType) {
        PolarAccountInfo user = getReservedUser(accountType);
        if (user == null) {
            throw new TddlRuntimeException(ERR_SERVER, "Reserved user: " + accountType.name() + " not exists!");
        }

        return user;
    }

    /**
     * Enable or disable rights separation mode.
     * <p>
     * When in rights separation mode, default roles will be granted to different root accounts.
     * </p>
     *
     * @param enabled Enable  separation or not.
     */
    public synchronized void changeRightsSeparationMode(boolean enabled) {
        this.rightsSeparationEnabled = enabled;

        if (enabled) {
            // dba
            updateReservedAccount(getAndCheckReservedUser(AccountType.DBA));
            updateReservedAccount(getAndCheckReservedUser(AccountType.SSO));
            updateReservedAccount(getAndCheckReservedUser(AccountType.AUDITOR));
        } else {
            // dba
            PolarAccountInfo oldDBAUser = getReservedUser(AccountType.DBA);
            if (oldDBAUser != null) {
                updateReservedAccount(oldDBAUser);
            }
            // sso
            PolarAccountInfo oldSSOUser = getReservedUser(AccountType.SSO);
            if (oldSSOUser != null) {
                updateReservedAccount(oldSSOUser);
            }
            // auditor
            PolarAccountInfo oldAuditorUser = getReservedUser(AccountType.AUDITOR);
            if (oldAuditorUser != null) {
                updateReservedAccount(oldAuditorUser);
            }
        }
    }

    @VisibleForTesting
    public void clear() {
        accountToIdMap.clear();
        userInfoMap.clear();
    }

    public boolean isReservedUser(PolarAccount account) {
        Preconditions.checkNotNull(account, "Account can't be null!");
        return reservedUsers.values()
            .stream()
            .map(PolarAccount::getUsername)
            .anyMatch(username -> username.equalsIgnoreCase(account.getUsername()));
    }

    public boolean isReservedRole(PolarAccount account) {
        Preconditions.checkNotNull(account, "Account can't be null!");
        return PolarReservedAccounts.isReservedRole(account);
    }

    public void consumeAccountInfos(Consumer<PolarAccountInfo> consumer) {
        userInfoMap.values().forEach(consumer);
    }

}

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

import java.util.Collection;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.ALTER;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.CREATE;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.CREATE_USER;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.CREATE_VIEW;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.DELETE;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.DROP;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.GRANT_OPTION;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.INDEX;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.INSERT;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.REPLICATION_CLIENT;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.REPLICATION_SLAVE;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.SELECT;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.SHOW_AUDIT_LOG;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.SHOW_VIEW;
import static com.alibaba.polardbx.gms.privilege.PrivilegeKind.UPDATE;

/**
 * This class contains specials accounts for rights separation.
 * <p>
 * We have three special account:
 * <p>
 *
 * </p>
 *
 * @author bairui.lrj
 * @since 5.4.10
 */
public class PolarReservedAccounts {
    private static final String ROLE_DBA = "polardbx_dba_role";
    private static final long ROLE_DBA_ID = -1;
    private static final EnumSet<PrivilegeKind> ROLE_DBA_PRIVILEGES = EnumSet.of(
        ALTER, CREATE, CREATE_VIEW, INDEX, DROP, SHOW_VIEW, GRANT_OPTION, REPLICATION_CLIENT, REPLICATION_SLAVE
    );
    static final PolarAccountInfo ROLE_DBA_ACCOUNT = createReservedRole(ROLE_DBA, ROLE_DBA_ID,
        ROLE_DBA_PRIVILEGES);
    private static final String ROLE_ADMIN = "polardbx_admin_role";
    private static final long ROLE_ADMIN_ID = -2;
    private static final EnumSet<PrivilegeKind> ROLE_ADMIN_PRIVILEGES = EnumSet.of(
        SHOW_VIEW, GRANT_OPTION, DELETE, INSERT, SELECT, UPDATE
    );
    static final PolarAccountInfo ROLE_ADMIN_ACCOUNT = createReservedRole(ROLE_ADMIN, ROLE_ADMIN_ID,
        ROLE_ADMIN_PRIVILEGES);
    private static final String ROLE_SECURITY_OFFICER = "polardbx_sso_role";
    private static final long ROLE_SECURITY_OFFICER_ID = -3;
    private static final EnumSet<PrivilegeKind> ROLE_SECURITY_OFFICER_PRIVILEGES = EnumSet.of(
        GRANT_OPTION, CREATE_USER
    );
    static final PolarAccountInfo ROLE_SECURITY_OFFICER_ACCOUNT = createReservedRole(ROLE_SECURITY_OFFICER,
        ROLE_SECURITY_OFFICER_ID, ROLE_SECURITY_OFFICER_PRIVILEGES);
    private static final String ROLE_AUDITOR = "polardbx_auditor_role";
    private static final long ROLE_AUDITOR_ID = -4;
    private static final EnumSet<PrivilegeKind> ROLE_AUDITOR_PRIVILEGES = EnumSet.of(
        GRANT_OPTION, SHOW_AUDIT_LOG
    );
    static final PolarAccountInfo ROLE_AUDITOR_ACCOUNT = createReservedRole(ROLE_AUDITOR, ROLE_AUDITOR_ID,
        ROLE_AUDITOR_PRIVILEGES);
    private static final Set<String> RESERVED_USERNAMES = Stream.of(
        ROLE_DBA_ACCOUNT.getAccount().getUsername(),
        ROLE_ADMIN_ACCOUNT.getAccount().getUsername(),
        ROLE_SECURITY_OFFICER_ACCOUNT.getAccount().getUsername(),
        ROLE_AUDITOR_ACCOUNT.getAccount().getUsername()
    ).map(String::toLowerCase)
        .collect(Collectors.toSet());

    private static PolarAccountInfo createReservedRole(String role, long roleId, Set<PrivilegeKind> privileges) {
        PolarAccount account = PolarAccount.newBuilder()
            .setAccountId(roleId)
            .setUsername(role)
            .setAccountType(AccountType.ROLE)
            .build();

        PolarAccountInfo accountInfo = new PolarAccountInfo(account);
        for (PrivilegeKind privilege : privileges) {
            accountInfo.getInstPriv().grantPrivilege(privilege);
        }

        return accountInfo;
    }

    static boolean isReservedRole(PolarAccount account) {
        return Optional.ofNullable(account)
            .flatMap(acc -> Optional.ofNullable(acc.getUsername()))
            .map(String::toLowerCase)
            .map(RESERVED_USERNAMES::contains)
            .orElse(false);
    }

    private static void checkReservedAccount(PolarAccount account) {
        if (isReservedRole(account)) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPERATION_NOT_ALLOWED,
                "Modify reserved account " + account.getIdentifier());
        }
    }

    public static void checkReservedAccounts(Collection<PolarAccountInfo> accounts) {
        accounts.stream()
            .map(PolarAccountInfo::getAccount)
            .forEach(PolarReservedAccounts::checkReservedAccount);
    }
}


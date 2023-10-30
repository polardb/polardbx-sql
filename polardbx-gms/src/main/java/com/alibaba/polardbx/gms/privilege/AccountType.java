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

import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * A user or role account.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/roles.html">Roles in mysql</a>
 * @since 5.4.9
 */
public enum AccountType {
    /**
     * User account.
     */
    USER((byte) 0),
    /**
     * Role account.
     */
    ROLE((byte) 1),

    /**
     * These three account types: DBA, SSO, AUDITOR are reserved accounts for rights separation.
     * They are still users, but with special protection rules. For rights separation
     */
    DBA((byte) 2),
    SSO((byte) 3),
    AUDITOR((byte) 4),

    /**
     * This user still has all permissions in spite of rights separation enabled.
     * Currently only polardbx_root@'%'
     */
    GOD((byte) 5);

    /**
     * Used to speed up looking up account type by id.
     */
    private static final Map<Byte, AccountType> ID_TO_ACCOUNT_TYPE = new HashMap<>();

    static {
        for (AccountType type : AccountType.values()) {
            ID_TO_ACCOUNT_TYPE.putIfAbsent(type.getId(), type);
        }
    }

    private static final EnumSet<AccountType> RESERVED_USER_TYPES = EnumSet.of(DBA, SSO, AUDITOR, GOD);

    /**
     * Serializable format of account so that it can be stored in GMS.
     */
    private final byte id;

    AccountType(byte id) {
        this.id = id;
    }

    /**
     * Lookup account type by its id.
     *
     * @param id Account type id.
     * @return Account type with {@code id}
     * @throws RuntimeException When no account type with {@code id} found.
     */
    public static AccountType lookupByCode(byte id) {
        AccountType accountType = ID_TO_ACCOUNT_TYPE.get(id);
        if (accountType == null) {
            throw GeneralUtil.nestedException("Account type with id: " + id + " not found!");
        }
        return accountType;
    }

    /**
     * Get id of account type.
     *
     * @return Id of account type.
     */
    public byte getId() {
        return id;
    }

    public boolean isReservedUser() {
        return RESERVED_USER_TYPES.contains(this);
    }

    public boolean isGod() {
        return AccountType.GOD == this;
    }

    public boolean isDBA() {
        return AccountType.DBA == this;
    }

    /**
     * 高权限账号
     * 包含 polardbx_root 与 DBA
     */
    public boolean isSuperUser() {
        return isGod() || isDBA();
    }

    public boolean isUser() {
        return ROLE != this;
    }

    public boolean isRole() {
        return !isUser();
    }
}

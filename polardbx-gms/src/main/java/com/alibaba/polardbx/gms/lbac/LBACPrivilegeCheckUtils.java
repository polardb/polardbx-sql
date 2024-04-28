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

package com.alibaba.polardbx.gms.lbac;

import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;
import com.alibaba.polardbx.lbac.LBACException;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class LBACPrivilegeCheckUtils {
    private static LBACRules lbACRules = new DefaultLBACRules();

    public static Set<Pair<String, String>> getTableWithPolicy(List<PrivilegeVerifyItem> items, String defaultSchema) {
        Set<Pair<String, String>> targetTables = new HashSet<>();
        if (items == null) {
            return targetTables;
        }
        for (PrivilegeVerifyItem item : items) {
            String db = item.getDb() == null ? defaultSchema : item.getDb();
            String table = item.getTable();
            if (LBACSecurityManager.getInstance().getTablePolicy(db, table) != null) {
                targetTables.add(new Pair<>(db, table));
            }
        }
        return targetTables;
    }

    public static boolean isNeedLBACCheck(Set<Pair<String, String>> accessTables, String defaultSchema) {
        if (accessTables == null || accessTables.isEmpty()) {
            return false;
        }
        for (Pair<String, String> table : accessTables) {
            if (LBACSecurityManager.getInstance()
                .getTablePolicy(table.getKey() == null ? defaultSchema : table.getKey(), table.getValue()) != null) {
                return true;
            }
        }
        return false;
    }

    public static boolean checkColumnRW(PolarAccount account, String schema, String table,
                                        Set<String> columns, boolean read) {

        LBACSecurityPolicy policy = LBACSecurityManager.getInstance().getTablePolicy(schema, table);
        //此表没有security policy，直接访问
        if (policy == null) {
            return true;
        }
        for (String column : columns) {
            LBACSecurityLabel colLabel = LBACSecurityManager.getInstance().getColumnLabel(schema, table, column);
            if (colLabel == null) {
                continue;
            }
            if (!checkUserRW(account, colLabel.getLabelName(), read)) {
                return false;
            }
        }

        return true;
    }

    /**
     * check user can read/write label
     */
    public static boolean checkUserRW(PolarAccount account, String labelName, boolean read) {

        if (labelName == null) {
            return true;
        }

        LBACSecurityLabel label = LBACSecurityManager.getInstance().getLabel(labelName);
        if (label == null) {
            throw new LBACException("security label is not exist");
        }
        LBACSecurityPolicy LBACSecurityPolicy = LBACSecurityManager.getInstance().getPolicy(label.getPolicyName());
        if (LBACSecurityPolicy == null) {
            throw new LBACException("security policy is not exist");
        }

        //高权限账户越过检查
        if (isHighPrivilege(account)) {
            return true;
        }
        LBACSecurityLabel
            userLabel =
            LBACSecurityManager.getInstance().getUserLabel(account, LBACSecurityPolicy.getPolicyName(), read);
        if (userLabel == null) {
            throw new LBACException("security label is not exist");
        }

        return checkSecurityLabelRW(LBACSecurityPolicy, userLabel, label, read);

    }

    /**
     * check label1 can read/write label2
     */
    public static boolean checkSecurityLabelRW(String labelName1, String labelName2, boolean read) {
        if (labelName2 == null) {
            return true;
        }
        if (labelName1 == null) {
            return false;
        }

        LBACSecurityLabel label1 = LBACSecurityManager.getInstance().getLabel(labelName1);
        LBACSecurityLabel label2 = LBACSecurityManager.getInstance().getLabel(labelName2);
        if (label1 == null || label2 == null) {
            throw new LBACException("security label is not exist");
        }
        if (!StringUtils.equals(label1.getPolicyName(), label2.getPolicyName())) {
            throw new LBACException("security labels are not belong to same policy");
        }
        LBACSecurityPolicy policy = LBACSecurityManager.getInstance().getPolicy(label1.getPolicyName());
        if (policy == null) {
            throw new LBACException("security policy is not exist");

        }
        return checkSecurityLabelRW(policy, label1, label2, read);

    }

    private static boolean checkSecurityLabelRW(LBACSecurityPolicy policy, LBACSecurityLabel label1,
                                                LBACSecurityLabel label2, boolean read) {

        for (int i = 0; i < policy.getComponentNames().length; i++) {
            LBACSecurityLabelComponent component =
                LBACSecurityManager.getInstance().getComponent(policy.getComponentNames()[i]);
            if (component == null) {
                throw new LBACException("security label component is not exist");
            }
            if ((read && !lbACRules.canRead(component, label1.getComponents()[i], label2.getComponents()[i]))
                || (!read && !lbACRules.canWrite(component, label1.getComponents()[i], label2.getComponents()[i]))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isHighPrivilege(AccountType type) {
        return type == AccountType.GOD || type == AccountType.SSO || type == AccountType.DBA;
    }

    public static boolean isHighPrivilege(PolarAccountInfo accountInfo) {
        return isHighPrivilege(accountInfo.getAccountType());
    }

    public static boolean isHighPrivilege(PolarAccount account) {
        return isHighPrivilege(account.getAccountType());
    }

}

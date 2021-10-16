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

package com.alibaba.polardbx.common.privilege;

import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.common.utils.PropertyReflectUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.taobao.tddl.common.privilege.EncrptPassword;
import com.taobao.tddl.common.privilege.GrantParameter;
import com.taobao.tddl.common.privilege.PrivilegeLevel;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.taobao.tddl.common.privilege.RevokeParameter;

import java.security.NoSuchAlgorithmException;
import java.util.SortedSet;

public class PrivilegeUtil {

    public static boolean isAllPrivs(SortedSet<PrivilegePoint> privPoints, PrivilegeLevel privLevel) {
        if (privPoints == null || privPoints.isEmpty()) {
            return false;
        }
        return privPoints.containsAll(privLevel.getPrivs());
    }

    public static void addPrivilegePointsToDbPriv(DbPriv dbPriv, GrantParameter grantParameter) {
        SortedSet<PrivilegePoint> privilegePoints = grantParameter.getPrivs();
        for (PrivilegePoint privilegePoint : privilegePoints) {
            String humpStylePrivName = TStringUtil.convertToHumpStr(privilegePoint.getColumnName());
            PropertyReflectUtil.setPropertyValue(dbPriv, humpStylePrivName, true);
        }
    }

    public static void removePrivilegePointsFromDbPriv(DbPriv dbPriv, RevokeParameter revokeParameter) {
        SortedSet<PrivilegePoint> privs = revokeParameter.getPrivs();
        for (PrivilegePoint priv : privs) {
            String humpStylePrivName = TStringUtil.convertToHumpStr(priv.getColumnName());
            PropertyReflectUtil.setPropertyValue(dbPriv, humpStylePrivName, false);
        }
    }

    public static void addPrivilegePointsToTbPriv(TbPriv tbPriv, GrantParameter grantParameter) {
        SortedSet<PrivilegePoint> privilegePoints = grantParameter.getPrivs();
        for (PrivilegePoint privilegePoint : privilegePoints) {
            String humpStylePrivName = TStringUtil.convertToHumpStr(privilegePoint.getColumnName());
            PropertyReflectUtil.setPropertyValue(tbPriv, humpStylePrivName, true);
        }
    }

    public static void removePrivilegePointsFromTbPriv(TbPriv tbPriv, RevokeParameter revokeParameter) {
        SortedSet<PrivilegePoint> privs = revokeParameter.getPrivs();
        for (PrivilegePoint priv : privs) {
            String humpStylePrivName = TStringUtil.convertToHumpStr(priv.getColumnName());
            PropertyReflectUtil.setPropertyValue(tbPriv, humpStylePrivName, false);
        }
    }

    public static String encryptPasswordWithSHA1(String rawInput) throws NoSuchAlgorithmException {
        if (rawInput == null) {
            throw new IllegalArgumentException("Illegal argument, rawInput cann't be null!");
        }

        return SecurityUtil.byte2HexStr(SecurityUtil.sha1Pass(rawInput.getBytes()));
    }

    public static EncrptPassword encryptPassword(String rawPassword) {
        String password = null;
        try {
            password = PrivilegeUtil.encryptPasswordWithSHA1(rawPassword);
        } catch (NoSuchAlgorithmException e) {

        }

        return new EncrptPassword(password, true);
    }

    public static String parseHost(String host) {
        if (TStringUtil.isEmpty(host) || "\"\"".equals(host) ||
            "''".equals(host)) {
            return Host.DEFAULT_HOST;
        } else {
            return host;
        }
    }
}

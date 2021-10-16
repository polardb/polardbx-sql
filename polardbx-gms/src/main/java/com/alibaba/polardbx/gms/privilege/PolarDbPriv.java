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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author shicai.xsc 2020/3/3 13:33
 * @since 5.0.0.0
 */
public class PolarDbPriv extends BasePolarPriv {

    private String dbName;

    public PolarDbPriv() {
        super(PrivilegeScope.DATABASE);
    }

    static Collection<PolarAccountInfo> loadDbPrivs(Connection conn, Collection<PolarAccount> accounts)
        throws SQLException {
        final String sql = String.format("SELECT * FROM %s WHERE %s = ? and %s = ?", PolarPrivUtil.DB_PRIV_TABLE, PolarPrivUtil.USER_NAME, PolarPrivUtil.HOST);
        List<PolarAccountInfo> accountInfos = new ArrayList<>(accounts.size());
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (PolarAccount account : accounts) {
                stmt.setString(1, account.getUsername());
                stmt.setString(2, account.getHost());

                PolarAccountInfo info = new PolarAccountInfo(account);
                try (ResultSet rs = stmt.executeQuery()) {
                    loadDbPriv(rs, info::addDbPriv);
                }
                accountInfos.add(info);
            }
        }

        return accountInfos;
    }

    static void loadDbPriv(ResultSet rs, Consumer<PolarDbPriv> consumer) throws SQLException {

        while (rs.next()) {
            PolarDbPriv dbPriv = new PolarDbPriv();
            BasePolarPriv.loadBasePriv(rs, dbPriv);
            dbPriv.setDbName(rs.getString(PolarPrivUtil.DB_NAME));
            consumer.accept(dbPriv);

//            // for information schema
//            PolarDbPriv dbPriv2 = new PolarDbPriv();
//            dbPriv2.setUserName(dbPriv.getUserName());
//            dbPriv2.setHost(dbPriv.getHost());
//            dbPriv2.setDbName(PolarPrivUtil.INFORMATION_SCHEMA);
//            dbPriv2.grantPrivilege(PrivilegeKind.SELECT);
//            consumer.accept(dbPriv2);
        }

    }

    public PolarDbPriv deepCopy() {
        PolarDbPriv clone = new PolarDbPriv();
        copy(this, clone);
        clone.dbName = this.dbName;
        return clone;
    }

    public boolean canCover(PolarDbPriv privToCover, boolean checkGrant) {
        return super.canCover(privToCover, checkGrant);
    }

    @Override
    public String getIdentifier() {
        return dbName.toLowerCase();
    }

    @Override
    public String toInsertNewSql() {
        return String.format("INSERT IGNORE INTO %s(%s, %s, %s) VALUES ('%s', '%s', '%s')",
            PolarPrivUtil.DB_PRIV_TABLE,
            PolarPrivUtil.USER_NAME,
            PolarPrivUtil.HOST,
            PolarPrivUtil.DB_NAME,
            userName,
            host,
            dbName);
    }

    public Optional<String> showGrantsResult(PolarAccount user) {
        return super.showGrantsResult(user, getIdentifier() + ".*", false);
    }

    public String getDbName() {
        return this.dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PolarDbPriv that = (PolarDbPriv) o;
        return Objects.equals(getDbName(), that.getDbName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDbName());
    }

    @Override
    public String toString() {
        return "PolarDbPriv(dbName=" + this.getDbName() + ")";
    }

    public Optional<String> toUpdatePrivilegeSql(boolean grant) {
        return toSetPrivilegeSql(grant)
            .map(setSql -> String.format("update %s set %s where %s = '%s' and %s = '%s' and %s = '%s'",
                PolarPrivUtil.DB_PRIV_TABLE, setSql, PolarPrivUtil.USER_NAME, userName, PolarPrivUtil.HOST, host, PolarPrivUtil.DB_NAME, dbName));
    }

    public static PolarDbPriv toInformationSchemaPriv(PolarAccount account) {
        PolarDbPriv priv = new PolarDbPriv();
        priv.setUserName(account.getUsername());
        priv.setHost(account.getHost());
        priv.setDbName(PolarPrivUtil.INFORMATION_SCHEMA);
        priv.grantPrivilege(PrivilegeKind.SELECT);

        return priv;
    }

    @Override
    public List<Permission> toPermissions() {
        return getGrantedPrivileges()
            .stream()
            .map(privilege -> Permission.databasePermission(getDbName(), privilege))
            .collect(Collectors.toList());
    }
}

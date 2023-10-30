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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shicai.xsc 2020/3/11 11:32
 * @since 5.0.0.0
 */
public class PolarPrivUtil {

    public static final String META_DB = "polardbx_meta_db";
    public static final String INFORMATION_SCHEMA = "information_schema";

    static final String ACCOUNT_ID = "id";
    public static final String USER_PRIV_TABLE = "user_priv";
    public static final String USER_NAME = "user_name";
    public static final String HOST = "host";
    public static final String ACCOUNT_TYPE = "account_type";
    public static final String PASSWORD = "password";
    public static final String SELECT_PRIV = "select_priv";
    public static final String INSERT_PRIV = "insert_priv";
    public static final String UPDATE_PRIV = "update_priv";
    public static final String DELETE_PRIV = "delete_priv";
    public static final String CREATE_PRIV = "create_priv";
    public static final String DROP_PRIV = "drop_priv";
    public static final String GRANT_PRIV = "grant_priv";
    public static final String INDEX_PRIV = "index_priv";
    public static final String ALTER_PRIV = "alter_priv";
    public static final String SHOW_VIEW_PRIV = "show_view_priv";
    public static final String CREATE_VIEW_PRIV = "create_view_priv";
    public static final String CREATE_USER_PRIV = "create_user_priv";
    public static final String REPLICATION_SLAVE_PRIV = "replication_slave_priv";
    public static final String REPLICATION_CLIENT_PRIV = "replication_client_priv";
    public static final String META_DB_PRIV = "meta_db_priv";

    public static final String DB_PRIV_TABLE = "db_priv";
    public static final String DB_NAME = "db_name";

    public static final String TABLE_PRIV_TABLE = "table_priv";
    public static final String TABLE_NAME = "table_name";

    public static final String POLAR_ROOT = "polardbx_root";

    public static final String SELECT_LOGIN_ERROR_INFO =
        "select * from `" + GmsSystemTables.USER_LOGIN_ERROR_LIMIT + "` where limit_key = ?";
    public static final String SELECT_ALL_LOGIN_ERROR_INFO =
        "select * from `" + GmsSystemTables.USER_LOGIN_ERROR_LIMIT + "`";
    public static final String INSERT_LOGIN_ERROR_INFO =
        "insert into `" + GmsSystemTables.USER_LOGIN_ERROR_LIMIT
            + "`(`limit_key`,`max_error_limit`,`error_count`) values (?,?,?)";
    public static final String UPDATE_LOGIN_ERROR_INFO =
        "update `" + GmsSystemTables.USER_LOGIN_ERROR_LIMIT
            + "` set error_count = ? ,expire_date = ?, max_error_limit = ? where limit_key = ? and error_count = ?";

    public static String getSelectAllUserPrivSql() {
        return String.format("select * from %s", USER_PRIV_TABLE);
    }

    public static String getSelectAllDbPrivSql() {
        return String.format("select * from %s", DB_PRIV_TABLE);
    }

    public static String getSelectAllTablePrivSql() {
        return String.format("select * from %s", TABLE_PRIV_TABLE);
    }

    public static String getLoadAllDbsSql() {
        return "select db_name, app_name from db_info where db_status=0";
    }

    public static String getCheckUserPrivSql(PolarAccountInfo userInfo) {
        return String.format("select * from %s where user_name='%s' and host='%s'",
            USER_PRIV_TABLE,
            userInfo.getUsername(),
            userInfo.getHost());
    }

    public static String getInsertUserPrivSql(PolarAccountInfo userInfo, boolean ifNotExists) {
        PolarInstPriv instPriv = userInfo.getInstPriv() == null ? new PolarInstPriv() : userInfo.getInstPriv();
        Pair<List<String>, List<String>> colAndValues = getPrivColAndValues(instPriv);

        colAndValues.getKey().add(USER_NAME);
        colAndValues.getKey().add(HOST);
        colAndValues.getKey().add(ACCOUNT_TYPE);
        colAndValues.getKey().add(PASSWORD);
        colAndValues.getValue().add(PolarPrivUtil.quote(userInfo.getUsername()));
        colAndValues.getValue().add(PolarPrivUtil.quote(userInfo.getHost()));
        colAndValues.getValue().add(PolarPrivUtil.quote(userInfo.getAccountType().getId() + ""));
        colAndValues.getValue().add(PolarPrivUtil.quote(userInfo.getPassword()));

        String ignoreDuplicates = "";
        if (ifNotExists) {
            ignoreDuplicates = "ignore";
        }
        return String.format("insert %s into %s(id, gmt_created, gmt_modified, %s) values(null, now(), now(), %s)",
            ignoreDuplicates,
            USER_PRIV_TABLE,
            String.join(",", colAndValues.getKey()),
            String.join(",", colAndValues.getValue()));
    }

    public static String getUpdateUserPrivSql(PolarAccountInfo userInfo, PrivManageType type) {
        List<String> colValues = new ArrayList<>();
        List<String> colNames = new ArrayList<>();

        switch (type) {
        case SET_PASSWORD:
            colNames.add("password");
            colValues.add(PolarPrivUtil.quote(userInfo.getPassword()));
            break;
        case GRANT_PRIVILEGE:
        case REVOKE_PRIVILEGE:
            Pair<List<String>, List<String>> colAndValues = getPrivColAndValues(userInfo.getInstPriv());
            colNames.add(CREATE_USER_PRIV);
            colNames.addAll(colAndValues.getKey());
            colValues.add(userInfo.getInstPriv().hasPrivilege(PrivilegeKind.CREATE_USER) ? "1" : "0");
            colValues.addAll(colAndValues.getValue());
            break;
        }

        return String.format("update %s set %s where %s='%s' and %s='%s'",
            USER_PRIV_TABLE,
            PolarPrivUtil.getSetSql(colNames, colValues),
            USER_NAME,
            userInfo.getUsername(),
            HOST,
            userInfo.getHost());
    }

    public static String getDeleteUserPrivSql(PolarAccountInfo userInfo) {
        return String.format("delete from %s where %s='%s' and %s='%s'",
            USER_PRIV_TABLE,
            USER_NAME,
            userInfo.getUsername(),
            HOST,
            userInfo.getHost());
    }

    public static String getCheckDbPrivSql(PolarDbPriv dbPriv) {
        return String.format("select * from %s where %s='%s' and %s='%s' and %s='%s'",
            DB_PRIV_TABLE,
            USER_NAME,
            dbPriv.getUserName(),
            HOST,
            dbPriv.getHost(),
            DB_NAME,
            dbPriv.getDbName());
    }

    public static String getInsertDbPrivSql(PolarDbPriv dbPriv) {
        Pair<List<String>, List<String>> colAndValues = getPrivColAndValues(dbPriv);

        colAndValues.getKey().add(USER_NAME);
        colAndValues.getKey().add(HOST);
        colAndValues.getKey().add(DB_NAME);
        colAndValues.getValue().add(PolarPrivUtil.quote(dbPriv.getUserName()));
        colAndValues.getValue().add(PolarPrivUtil.quote(dbPriv.getHost()));
        colAndValues.getValue().add(PolarPrivUtil.quote(dbPriv.getDbName()));

        return String.format("insert into %s(id, gmt_created, gmt_modified, %s) values(null, now(), now(), %s)",
            DB_PRIV_TABLE,
            String.join(",", colAndValues.getKey()),
            String.join(",", colAndValues.getValue()));
    }

    public static String getDeleteDbPrivSql(PolarDbPriv dbPriv) {
        return String.format("delete from %s where %s='%s' and %s='%s' and %s='%s'",
            DB_PRIV_TABLE,
            USER_NAME,
            dbPriv.getUserName(),
            HOST,
            dbPriv.getHost(),
            DB_NAME,
            dbPriv.getDbName());
    }

    public static String getDeleteAllDbPrivSql(PolarAccountInfo userInfo) {
        return String.format("delete from %s where %s='%s' and %s='%s'",
            DB_PRIV_TABLE,
            USER_NAME,
            userInfo.getUsername(),
            HOST,
            userInfo.getHost());
    }

    public static String getUpdateDbPrivSql(PolarDbPriv dbPriv) {
        Pair<List<String>, List<String>> colAndValues = getPrivColAndValues(dbPriv);

        return String.format("update %s set %s where %s='%s' and %s='%s' and %s='%s'",
            DB_PRIV_TABLE,
            PolarPrivUtil.getSetSql(colAndValues.getKey(), colAndValues.getValue()),
            USER_NAME,
            dbPriv.getUserName(),
            HOST,
            dbPriv.getHost(),
            DB_NAME,
            dbPriv.getDbName());
    }

    public static String getCheckTablePrivSql(PolarTbPriv tbPriv) {
        return String.format("select * from %s where user_name='%s' and host='%s' and db_name='%s' and table_name='%s'",
            TABLE_PRIV_TABLE,
            tbPriv.getUserName(),
            tbPriv.getHost(),
            tbPriv.getDbName(),
            tbPriv.getTbName());
    }

    public static String getInsertTablePrivSql(PolarTbPriv tbPriv) {
        Pair<List<String>, List<String>> colAndValues = getPrivColAndValues(tbPriv);

        colAndValues.getKey().add(USER_NAME);
        colAndValues.getKey().add(HOST);
        colAndValues.getKey().add(DB_NAME);
        colAndValues.getKey().add(TABLE_NAME);
        colAndValues.getValue().add(PolarPrivUtil.quote(tbPriv.getUserName()));
        colAndValues.getValue().add(PolarPrivUtil.quote(tbPriv.getHost()));
        colAndValues.getValue().add(PolarPrivUtil.quote(tbPriv.getDbName()));
        colAndValues.getValue().add(PolarPrivUtil.quote(tbPriv.getTbName()));

        return String.format("insert into %s(id, gmt_created, gmt_modified, %s) values(null, now(), now(), %s)",
            TABLE_PRIV_TABLE,
            String.join(",", colAndValues.getKey()),
            String.join(",", colAndValues.getValue()));
    }

    public static String getDeleteTablePrivSql(PolarTbPriv tbPriv) {
        return String.format("delete from %s where %s='%s' and %s='%s' and %s='%s' and %s='%s'",
            TABLE_PRIV_TABLE,
            USER_NAME,
            tbPriv.getUserName(),
            HOST,
            tbPriv.getHost(),
            DB_NAME,
            tbPriv.getDbName(),
            TABLE_NAME,
            tbPriv.getTbName());
    }

    public static String getDeleteAllTablePrivSql(PolarAccountInfo userInfo) {
        return String.format("delete from %s where %s='%s' and %s='%s'",
            TABLE_PRIV_TABLE,
            USER_NAME,
            userInfo.getUsername(),
            HOST,
            userInfo.getHost());
    }

    public static String getUpdateTablePrivSql(PolarTbPriv tbPriv) {
        Pair<List<String>, List<String>> colAndValues = getPrivColAndValues(tbPriv);
        return String.format("update %s set %s where %s='%s' and %s='%s' and %s='%s' and %s='%s'",
            TABLE_PRIV_TABLE,
            PolarPrivUtil.getSetSql(colAndValues.getKey(), colAndValues.getValue()),
            USER_NAME,
            tbPriv.getUserName(),
            HOST,
            tbPriv.getHost(),
            DB_NAME,
            tbPriv.getDbName(),
            TABLE_NAME,
            tbPriv.getTbName());
    }

    public static Pair<List<String>, List<String>> getPrivColAndValues(BasePolarPriv priv) {
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (PrivilegeKind kind : priv.allPrivilegeKinds()) {
            names.add(kind.getColumnName());
            values.add(priv.hasPrivilege(kind) ? "1" : "0");
        }
        return new Pair<>(names, values);
    }

    public static PrivManageLevel getPrivManageLevel(PolarAccountInfo userInfo) {
        if (userInfo.getInstPriv().hasAnyPrivilege()) {
            return PrivManageLevel.INST;
        } else if (!userInfo.getDbPrivMap().isEmpty()) {
            return PrivManageLevel.DB;
        } else {
            return PrivManageLevel.TABLE;
        }
    }

//    private static Permission toPermission(PolarAccountInfo userInfo) {
//        if (userInfo.getInstPriv().hasAnyPrivilege()) {
//            return PrivManageLevel.INST;
//        } else if (!userInfo.getDbPrivMap().isEmpty()) {
//            return PrivManageLevel.DB;
//        } else {
//            return PrivManageLevel.TABLE;
//        }
//    }

    public static String quote(String value) {
        return "'" + value + "'";
    }

    public static String getSetSql(List<String> colNames, List<String> colValues) {
        List<String> nameValuePairs = new ArrayList<>();
        for (int i = 0; i < colNames.size(); i++) {
            nameValuePairs.add(colNames.get(i) + "=" + colValues.get(i));
        }
        return String.join(",", nameValuePairs);
    }

    public static int getCommonPrefixLength(String host1, String host2) {
        String prefix = StringUtils.getCommonPrefix(new String[] {host1, host2});
        return prefix.length();
    }

    public static boolean isPolarxRootUser(String user) {
        return POLAR_ROOT.equalsIgnoreCase(user);
    }

    public static void checkRootOnlyPriv(String user, String db) {
        if (!isPolarxRootUser(user)) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_DB,
                user, "%", db);
        }
    }
}

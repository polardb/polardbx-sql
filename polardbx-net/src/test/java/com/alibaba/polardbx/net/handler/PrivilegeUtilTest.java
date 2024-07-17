package com.alibaba.polardbx.net.handler;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.net.util.PrivilegeUtil;
import com.taobao.tddl.common.privilege.EncrptPassword;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author fangwu
 */
public class PrivilegeUtilTest {
    private final static String schema1 = "test_db1";
    private final static String schema2 = "test_db2";
    private final static String schema3 = "test_db3";

    /**
     * test com.alibaba.polardbx.net.util.PrivilegeUtil#checkSchema
     */
    @Test
    public void testCheckSchema() {
        String user = "test_user";
        String host = "test_user";
        // schema is null
        assert null == PrivilegeUtil.checkSchema(null, user, host, true, getPrivileges());

        // schema not exists
        assert ErrorCode.ER_BAD_DB_ERROR == PrivilegeUtil.checkSchema(schema3, user, host, true, getPrivileges());

        // trust login
        assert null == PrivilegeUtil.checkSchema(schema2, user, host, true, getPrivileges());

        // user has privilege
        assert null == PrivilegeUtil.checkSchema(schema1, user, host, false, getPrivileges());

        // user has no privilege
        assert ErrorCode.ER_DBACCESS_DENIED_ERROR == PrivilegeUtil.checkSchema(schema2, user, host, false,
            getPrivileges());
    }

    private Privileges getPrivileges() {
        return new Privileges() {
            private final Set<String> schemas = new HashSet<>();

            {
                schemas.add(schema1);
                schemas.add(schema2);
            }

            @Override
            public boolean schemaExists(String schema) {
                return schemas.contains(schema);
            }

            @Override
            public boolean userExists(String user) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public boolean userExists(String user, String host) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public boolean userMatches(String user, String host) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public boolean checkQuarantine(String user, String host) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public EncrptPassword getPassword(String user) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public EncrptPassword getPassword(String user, String host) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public Set<String> getUserSchemas(String user) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public Set<String> getUserSchemas(String user, String host) {
                Set<String> userDb = new HashSet<>();
                userDb.add(schema1);
                return userDb;
            }

            @Override
            public boolean isTrustedIp(String host, String user) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public Map<String, DbPriv> getSchemaPrivs(String user, String host) {
                throw new RuntimeException("Unexpected execution path.");
            }

            @Override
            public Map<String, TbPriv> getTablePrivs(String user, String host, String database) {
                throw new RuntimeException("Unexpected execution path.");
            }
        };
    }
}

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

import com.alibaba.polardbx.common.privilege.Host;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateRoleStatement;
import com.taobao.tddl.common.privilege.AuthPlugin;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * An account is a user or role.
 *
 * @author bairui.lrj
 * @see AccountType
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/roles.html">Roles in mysql</a>
 * @since 5.4.9
 */
public class PolarAccount {
    private static final String DEFAULT_HOST = "%";
    private final Long accountId;
    private final String username;
    private final String host;

    /**
     * Encrypted password.
     */
    private final String password;
    private final AuthPlugin authPlugin;
    private final AccountType accountType;

    private final Predicate<String> hostMatcher;

    private PolarAccount(Builder builder) {
        this.accountId = builder.accountId;
        this.username = builder.username;
        this.host = checkHost(builder.host);
        this.password = builder.password;
        this.accountType = Optional.ofNullable(builder.accountType).orElse(AccountType.USER);
        this.authPlugin = Optional.ofNullable(builder.authPlugin).orElse(AuthPlugin.POLARDBX_NATIVE_PASSWORD);
        this.hostMatcher = Optional.ofNullable(accountType)
            .map(t -> getHostMatcher(accountType, host))
            .orElse(null);
    }

    private static String checkHost(String host) {
        if (host == null) {
            return DEFAULT_HOST;
        }
        if (!Host.verify(host)) {
            throw new IllegalArgumentException("Illegal host: " + host);
        }

        return host;
    }

    private static Predicate<String> getHostMatcher(final AccountType accountType, final String host) {
        if (accountType.isUser()) {
            return new Host(host);
        } else {
            return s -> Objects.equals(s, host);
        }
    }

    public PolarAccount deepCopy() {
        return copyBuilder(this).build();
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder copyBuilder(PolarAccount originAccount) {
        return newBuilder()
            .setAccountId(originAccount.getAccountId())
            .setUsername(originAccount.getUsername())
            .setHost(originAccount.getHost())
            .setPassword(originAccount.getPassword())
            .setAuthPlugin(originAccount.getAuthPlugin())
            .setAccountType(originAccount.getAccountType());
    }

    public static PolarAccount fromRoleSpec(MySqlCreateRoleStatement.RoleSpec roleSpec) {
        MySqlUserName mysqlUserName = roleSpec.getUsername();
        return newBuilder()
            .setUsername(mysqlUserName.getUserName())
            .setHost(mysqlUserName.getHost())
            .setAccountType(AccountType.ROLE)
            .build();
    }

    public static PolarAccount fromMySqlUsername(MySqlUserName mysqlUsername) {
        return newBuilder()
            .setAccountType(AccountType.ROLE)
            .setUsername(mysqlUsername.getUserName())
            .setHost(mysqlUsername.getHost())
            .build();
    }

    public static PolarAccount fromIdentifier(String identifier) {
        String[] parts = identifier.split("@", 2);
        Builder builder = newBuilder()
            .setUsername(unquote(parts[0]));

        if (parts.length > 1) {
            builder.setHost(unquote(parts[1]));
        }

        return builder.build();
    }

    public static String unquote(String part) {
        if (part.startsWith("'") && part.endsWith("'")) {
            return part.substring(1, part.length() - 1);
        }
        return part;
    }

    public String getUsername() {
        return username;
    }

    public String getHost() {
        return host;
    }

    public String getPassword() {
        return password;
    }

    public AuthPlugin getAuthPlugin() {
        return authPlugin;
    }

    public AccountType getAccountType() {
        return accountType;
    }

    public Long getAccountId() {
        return accountId;
    }

    public String getIdentifier() {
        return String.format("'%s'@'%s'", username, host);
    }

    public static String identifierOf(MySqlUserName mysqlUsername) {
        String host = Optional.ofNullable(mysqlUsername.getHost())
            .orElse("%");
        return String.format("'%s'@'%s'", mysqlUsername.getUserName(), host);
    }

    public boolean matches(String username, String host) {
        return StringUtils.equalsIgnoreCase(username, this.username)
            && this.hostMatcher.test(host);
    }

    public PolarAccount updatePassword(String newPassword) {
        return copyBuilder(this)
            .setPassword(newPassword)
            .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PolarAccount that = (PolarAccount) o;
        return Objects.equals(getUsername(), that.getUsername())
            && Objects.equals(getHost(), that.getHost());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUsername(), getHost());
    }

    @Override
    public String toString() {
        return "PolarAccount{" +
            "accountId=" + accountId +
            "username='" + username + '\'' +
            ", host='" + host + '\'' +
            ", password='" + password + '\'' +
            ", accountType=" + accountType +
            ", authPlugin=" + authPlugin +
            '}';
    }

    public static class Builder {
        private Long accountId;
        private AccountType accountType;
        private String username;
        private String host;
        private String password;
        private AuthPlugin authPlugin;

        public Builder setAccountId(Long accountId) {
            this.accountId = accountId;
            return this;
        }

        public Builder setAccountType(AccountType accountType) {
            this.accountType = accountType;
            return this;
        }

        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setAuthPlugin(AuthPlugin authPlugin) {
            this.authPlugin = authPlugin;
            return this;
        }

        public PolarAccount build() {
            return new PolarAccount(this);
        }
    }
}

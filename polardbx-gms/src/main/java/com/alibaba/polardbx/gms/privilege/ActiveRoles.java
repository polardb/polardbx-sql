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

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSetRoleStatement;

import java.util.Collections;
import java.util.Set;

/**
 * @author bairui.lrj
 * @since 5.4.9
 */
public class ActiveRoles {
    /**
     * Active roles spec for current user session.
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html">Set Role</a>
     * @see MySqlSetRoleStatement
     */
    public enum ActiveRoleSpec {
        /**
         * No active roles.
         */
        NONE,
        /**
         * Activate all granted roles.
         */
        ALL,
        /**
         * Activate all default roles.
         */
        DEFAULT,
        /**
         * Activate all roles except some.
         */
        ALL_EXCEPT,

        /**
         * Activate roles.
         */
        ROLES;

        public static ActiveRoleSpec from(MySqlSetRoleStatement.RoleSpec mysqlRoleSpec) {
            switch (mysqlRoleSpec) {
            case NONE:
                return NONE;
            case ALL:
                return ALL;
            case DEFAULT:
                return DEFAULT;
            case ALL_EXCEPT:
                return ALL_EXCEPT;
            case ROLES:
                return ROLES;
            default:
                throw new IllegalArgumentException("Unrecognized active role spec: " + mysqlRoleSpec);
            }
        }
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/set-role.html">Set Role</a>
     */
    private final ActiveRoles.ActiveRoleSpec activeRoleSpec;
    /**
     * Role list for active role spec. Useful when {@link #activeRoleSpec} is {@link
     * ActiveRoles.ActiveRoleSpec#ALL_EXCEPT} or {@link ActiveRoles.ActiveRoleSpec#ROLES}.
     */
    private final Set<Long> roles;

    public ActiveRoles(ActiveRoleSpec activeRoleSpec, Set<Long> roles) {
        this.activeRoleSpec = activeRoleSpec;
        this.roles = roles;
    }

    public ActiveRoleSpec getActiveRoleSpec() {
        return activeRoleSpec;
    }

    public Set<Long> getRoles() {
        return Collections.unmodifiableSet(roles);
    }

    public static ActiveRoles defaultValue() {
        return new ActiveRoles(ActiveRoleSpec.DEFAULT, Collections.emptySet());
    }

    public static ActiveRoles noneValue() {
        return new ActiveRoles(ActiveRoleSpec.NONE, Collections.emptySet());
    }
}

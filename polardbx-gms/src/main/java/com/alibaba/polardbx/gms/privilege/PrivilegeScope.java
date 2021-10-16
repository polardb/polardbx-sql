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

import java.util.EnumSet;

/**
 * Scope a privilege can belong to.
 *
 * @author bairui.lrj
 * @see PrivilegeKind
 * @since 5.4.9
 */
public enum PrivilegeScope {
    /**
     * <code>GRANT ALL ON <b>*.*</b> TO user;</code>
     */
    INSTANCE,
    /**
     * <code>GRANT ALL ON <b>a.*</b> TO user;</code> where a is a database name.
     */
    DATABASE,
    /**
     * <code>GRANT ALL ON <b>a.b</b> TO user;</code> where a is a database name, and b is a table name.
     */
    TABLE;

    public static EnumSet<PrivilegeScope> ALL = EnumSet.allOf(PrivilegeScope.class);
    public static EnumSet<PrivilegeScope> INSTANCE_ONLY = EnumSet.of(INSTANCE);
}

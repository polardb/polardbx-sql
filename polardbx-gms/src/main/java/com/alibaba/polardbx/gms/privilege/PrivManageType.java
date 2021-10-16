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

/**
 * @author shicai.xsc 2020/3/5 21:39
 * @since 5.0.0.0
 */
public enum PrivManageType {
    /**
     * Create user or role.
     *
     * @see AccountType
     */
    CREATE_ACCOUNT,

    /**
     * Drop user or role.
     *
     * @see AccountType
     */
    DROP_ACCOUNT,

    GRANT_PRIVILEGE,
    REVOKE_PRIVILEGE,
    SET_PASSWORD
}

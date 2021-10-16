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

package com.alibaba.polardbx.gms.privilege.authorize;

import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;

import java.util.Optional;

/**
 * One node in a rule chain to check whether user has required permission.
 *
 * @author bairui.lrj
 * @see PolarAuthorizer
 * @since 5.4.10
 */
interface Rule {
    /**
     * Checks whether an account has required permission.
     *
     * @param accountInfo Account to check.
     * @param permission Required permission.
     * @return Whether has permission.
     */
    boolean check(PolarAccountInfo accountInfo, Permission permission);

    /**
     * Next rule in rule chain.
     *
     * @return Next rule in rule chain.
     */
    Optional<Rule> getNext();

    /**
     * Set next rule in rule chain.
     *
     * @param rule Next rule in rule chain.
     */
    void setNext(Rule rule);
}

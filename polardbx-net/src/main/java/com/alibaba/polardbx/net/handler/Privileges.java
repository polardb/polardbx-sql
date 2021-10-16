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

package com.alibaba.polardbx.net.handler;

import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.taobao.tddl.common.privilege.EncrptPassword;

/**
 * 权限提供者
 *
 * @author xianmao.hexm
 */
public interface Privileges {

    /**
     * 检查schema是否存在
     */
    boolean schemaExists(String schema);

    /**
     * 检查用户是否存在，并且可以使用host实行隔离策略。
     */
    boolean userExists(String user);

    /**
     * 判断user是否存在, 注意user@host必须完全匹配才行.
     */
    boolean userExists(String user, String host);

    /**
     * 检查用户是否匹配, 并且可以使用host实行隔离策略.
     */
    boolean userMatches(String user, String host);

    /**
     * 检查是否为信任ip
     */
    boolean checkQuarantine(String user, String host);

    /**
     * 提供用户的服务器端密码
     */
    EncrptPassword getPassword(String user);

    /**
     * 返回用户的服务器端密码, 如果有多个匹配, 则返回第一个匹配用户的密码.
     */
    EncrptPassword getPassword(String user, String host);

    /**
     * 提供有效的用户schema集合
     */
    Set<String> getUserSchemas(String user);

    /**
     * 提供有效的用户schema集合
     */
    Set<String> getUserSchemas(String user, String host);

    /**
     * 判断是否是信任的白名单IP，可以没有配置信任白名单
     */
    boolean isTrustedIp(String host, String user);

    /**
     * 获取库级权限
     */
    Map<String/*dbName*/, DbPriv> getSchemaPrivs(String user, String host);

    /**
     * 获取表级权限
     */
    Map<String/*tbName*/, TbPriv> getTablePrivs(String user, String host, String database);
}

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

package com.alibaba.polardbx.mock.server;

import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.taobao.tddl.common.privilege.EncrptPassword;

import java.util.Map;
import java.util.Set;

/**
 * Mock Privileges
 *
 * @author changyuan.lh 2019年2月27日 上午12:14:48
 * @since 5.0.0
 */
public class MockPrivileges implements Privileges {

    @Override
    public boolean schemaExists(String schema) {
        return true;
    }

    @Override
    public boolean userExists(String user) {
        return true;
    }

    @Override
    public boolean userExists(String user, String host) {
        return true;
    }

    @Override
    public boolean userMatches(String user, String host) {
        return true;
    }

    @Override
    public boolean checkQuarantine(String user, String host) {
        return true;
    }

    @Override
    public EncrptPassword getPassword(String user) {
        return null;
    }

    @Override
    public EncrptPassword getPassword(String user, String host) {
        return null;
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        return null;
    }

    @Override
    public Set<String> getUserSchemas(String user, String host) {
        return null;
    }

    @Override
    public boolean isTrustedIp(String host, String user) {
        return true;
    }

    @Override
    public Map<String, DbPriv> getSchemaPrivs(String user, String host) {
        return null;
    }

    @Override
    public Map<String, TbPriv> getTablePrivs(String user, String host, String database) {
        return null;
    }
}

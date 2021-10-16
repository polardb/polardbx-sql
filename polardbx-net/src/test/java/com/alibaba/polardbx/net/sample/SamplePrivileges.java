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

package com.alibaba.polardbx.net.sample;

import com.alibaba.polardbx.net.handler.Privileges;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.taobao.tddl.common.privilege.EncrptPassword;

import java.util.Map;
import java.util.Set;

/**
 * @author xianmao.hexm
 */
public class SamplePrivileges implements Privileges {

    @Override
    public boolean schemaExists(String schema) {
        SampleConfig conf = SampleServer.getInstance().getConfig();
        return conf.getSchemas().contains(schema);
    }

    @Override
    public boolean userExists(String user) {
        SampleConfig conf = SampleServer.getInstance().getConfig();
        return conf.getUsers().containsKey(user);
    }

    @Override
    public boolean userExists(String user, String host) {
        return false;
    }

    @Override
    public boolean userMatches(String user, String host) {
        return false;
    }

    @Override
    public EncrptPassword getPassword(String user) {
        SampleConfig conf = SampleServer.getInstance().getConfig();
        return new EncrptPassword(conf.getUsers().get(user), false);
    }

    @Override
    public EncrptPassword getPassword(String user, String host) {
        return null;
    }

    @Override
    public Set<String> getUserSchemas(String user) {
        SampleConfig conf = SampleServer.getInstance().getConfig();
        return conf.getUserSchemas().get(user);
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

    @Override
    public boolean checkQuarantine(String user, String host) {
        return true;
    }

}

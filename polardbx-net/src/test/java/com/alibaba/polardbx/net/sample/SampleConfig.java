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

import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 服务器配置信息示例
 *
 * @author xianmao.hexm
 */
public class SampleConfig {

    /**
     * 服务器名
     */
    private String serverName;

    /**
     * 服务器版本
     */
    private String serverVersion;

    /**
     * 可登录的用户和密码
     */
    private Map<String, String> users;

    /**
     * 可使用的schemas
     */
    private Set<String> schemas;

    /**
     * 指定用户可使用的schemas
     */
    private Map<String, Set<String>> userSchemas;

    private Map<String/*user*/, Map<String/*dbName*/, DbPriv>> userSchemaPrivs;

    private Map<String/*user*/, Map<String/*dbName*/, Map<String/*tbName*/, TbPriv>>> userSchemaTbPrivs;

    public SampleConfig() {
        this.serverName = "Sample";
        this.serverVersion = "1.2.3";

        // add user/password
        this.users = new HashMap<String, String>();
        this.users.put("root", "12345");
        this.users.put("test", null);

        // add schema
        this.schemas = new HashSet<String>();
        this.schemas.add("schema1");
        this.schemas.add("schema2");
        this.schemas.add("schema3");

        // add user/schema
        this.userSchemas = new HashMap<String, Set<String>>();
        Set<String> schemaSet = new HashSet<String>();
        schemaSet.add("schema1");
        schemaSet.add("schema3");
        this.userSchemas.put("test", schemaSet);

        this.userSchemaPrivs = new HashMap<String, Map<String, DbPriv>>();
        Map<String, DbPriv> schemaPrivSet = new HashMap<String, DbPriv>();
        DbPriv dbPriv1 = new DbPriv("schema1");
        dbPriv1.setInsertPriv(false);
        schemaPrivSet.put("schema1", dbPriv1);
        DbPriv dbPriv2 = new DbPriv("schema2");
        dbPriv2.setUpdatePriv(false);
        schemaPrivSet.put("schema2", dbPriv2);
        this.userSchemaPrivs.put("test", schemaPrivSet);

        this.userSchemaTbPrivs = Maps.newHashMap();
        Map<String, Map<String, TbPriv>> schemaTbPrivMap1 = Maps.newHashMap();
        userSchemaTbPrivs.put("user1", schemaTbPrivMap1);

        Map<String, TbPriv> tbPrivMap1 = Maps.newHashMap();
        schemaTbPrivMap1.put("schema1", tbPrivMap1);
        tbPrivMap1.put("table1_1", new TbPriv("user1", "schema1"));
        tbPrivMap1.put("table1_2", new TbPriv("user1", "schema1"));
        tbPrivMap1.put("table1_3", new TbPriv("user1", "schema1"));

        Map<String, TbPriv> tbPrivMap2 = Maps.newHashMap();
        schemaTbPrivMap1.put("schema2", tbPrivMap2);
        tbPrivMap1.put("table2_1", new TbPriv("user1", "schema2"));
        tbPrivMap1.put("table2_2", new TbPriv("user1", "schema2"));

        Map<String, Map<String, TbPriv>> schemaTbPrivMap2 = Maps.newHashMap();
        userSchemaTbPrivs.put("user2", schemaTbPrivMap2);

        Map<String, TbPriv> tbPrivMap3 = Maps.newHashMap();
        schemaTbPrivMap2.put("schema3", tbPrivMap3);
        tbPrivMap3.put("table3_1", new TbPriv("user2", "schema3"));
        tbPrivMap3.put("table3_2", new TbPriv("user2", "schema3"));
        tbPrivMap3.put("table3_3", new TbPriv("user2", "schema3"));

        Map<String, TbPriv> tbPrivMap4 = Maps.newHashMap();
        schemaTbPrivMap2.put("schema4", tbPrivMap4);
        tbPrivMap3.put("table4_1", new TbPriv("user2", "schema4"));
        tbPrivMap3.put("table4_2", new TbPriv("user2", "schema4"));
    }

    public String getServerName() {
        return serverName;
    }

    public String getServerVersion() {
        return serverVersion;
    }

    public Map<String, String> getUsers() {
        return users;
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public Map<String, Set<String>> getUserSchemas() {
        return userSchemas;
    }

    public Map<String, Map<String, DbPriv>> getUserSchemaPrivs() {
        return userSchemaPrivs;
    }

    public Map<String/*user*/, Map<String/*dbName*/, Map<String/*tbName*/, TbPriv>>> getUserSchemaTbPrivs() {
        return userSchemaTbPrivs;
    }
}

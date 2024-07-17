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

package com.alibaba.polardbx.gms.lbac;

/**
 * @author pangzhaoxing
 */
public class LBACSecurityEntity {

    private int id;

    private EntityKey entityKey;

    private EntityType type;

    private String securityAttr;

    public LBACSecurityEntity() {
    }

    public LBACSecurityEntity(LBACSecurityEntity esa) {
        this.id = esa.getId();
        this.entityKey = esa.getEntityKey();
        this.type = esa.getType();
        this.securityAttr = esa.getSecurityAttr();
    }

    public LBACSecurityEntity(EntityKey entityKey, EntityType type, String securityAttr) {
        this.entityKey = entityKey;
        this.type = type;
        this.securityAttr = securityAttr;
    }

    public LBACSecurityEntity(int id, EntityKey entityKey, EntityType type, String securityAttr) {
        this.id = id;
        this.entityKey = entityKey;
        this.type = type;
        this.securityAttr = securityAttr;
    }

    public LBACSecurityEntity(EntityType type, EntityKey entityKey) {
        this.entityKey = entityKey;
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public EntityType getType() {
        return type;
    }

    public String getSecurityAttr() {
        return securityAttr;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setType(EntityType type) {
        this.type = type;
    }

    public void setSecurityAttr(String securityAttr) {
        this.securityAttr = securityAttr;
    }

    public EntityKey getEntityKey() {
        return entityKey;
    }

    public void setEntityKey(EntityKey entityKey) {
        this.entityKey = entityKey;
    }

    public static enum EntityType {
        USER_READ, USER_WRITE, TABLE, COLUMN
    }

    public static class EntityKey {
        String schema;
        String table;
        String column;
        String user;
        String policy;

        public EntityKey(String schema, String table, String column, String user, String policy) {
            this.schema = schema;
            this.table = table;
            this.column = column;
            this.user = user;
            this.policy = policy;
        }

        public static EntityKey createTableKey(String schema, String table) {
            return new EntityKey(schema, table, null, null, null);
        }

        public static EntityKey createColumnKey(String schema, String table, String column) {
            return new EntityKey(schema, table, column, null, null);
        }

        public static EntityKey createUserKey(String user, String policy) {
            return new EntityKey(null, null, null, user, policy);
        }

        public String getSchema() {
            return schema;
        }

        public String getTable() {
            return table;
        }

        public String getColumn() {
            return column;
        }

        public String getUser() {
            return user;
        }

        public String getPolicy() {
            return policy;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public void setPolicy(String policy) {
            this.policy = policy;
        }
    }

}

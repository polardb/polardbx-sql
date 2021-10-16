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

package com.alibaba.polardbx.common.model;

import com.taobao.tddl.common.utils.TddlToStringStyle;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Group {

    private String name;

    private String appName;

    private String schemaName;

    public enum GroupType {
        MYSQL_JDBC;

        public boolean isMysql() {
            return this.equals(MYSQL_JDBC);
        }
    }

    private GroupType type = GroupType.MYSQL_JDBC;

    private List<Atom> atoms = new ArrayList<Atom>();

    private Map<String, String> properties = new HashMap<>();

    private String unitName;

    private boolean enforceMaster = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public GroupType getType() {
        return type;
    }

    public void setType(GroupType type) {
        this.type = type;
    }

    public List<Atom> getAtoms() {
        return atoms;
    }

    public Atom getAtom(String atomName) {
        for (Atom atom : atoms) {
            if (atom.getName().equals(atomName)) {
                return atom;
            }
        }
        return null;

    }

    public void setAtoms(List<Atom> atoms) {
        this.atoms = atoms;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getUnitName() {
        return this.unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isEnforceMaster() {
        return enforceMaster;
    }

    public void setEnforceMaster(boolean enforceMaster) {
        this.enforceMaster = enforceMaster;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Group group = (Group) o;
        return Objects.equals(name, group.name) &&
            Objects.equals(appName, group.appName) &&
            Objects.equals(schemaName, group.schemaName) &&
            type == group.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, appName, schemaName, type);
    }
}

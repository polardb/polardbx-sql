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


public class Matrix {

    private String name;
    private String schemaName;
    private volatile List<Group> groups = new ArrayList<Group>();

    private volatile List<Group> scaleOutGroups = new ArrayList<Group>();

    private Map<String, String> properties = new HashMap();
    private List<Matrix> subMatrixs = new ArrayList();
    private Map<String, RepoInst> masterRepoInstMap = new HashMap<String, RepoInst>();

    public Matrix() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Group> getGroups() {
        return groups;
    }

    public void setGroups(List<Group> groups) {
        this.groups = groups;
    }

    public void setScaleOutGroups(List<Group> groups) {
        this.scaleOutGroups = groups;
    }

    public List<Group> getScaleOutGroups() {
        return this.scaleOutGroups;
    }

    public void addScaleOutGroups(List<Group> scaleOutGroups) {
        this.scaleOutGroups.addAll(scaleOutGroups);
    }

    public Group getGroup(String groupName) {

        for (Group group : groups) {
            if (group.getName().equalsIgnoreCase(groupName)) {
                return group;
            }
        }

        for (Matrix subMatrix : this.subMatrixs) {
            Group group = subMatrix.getGroup(groupName);

            if (group != null) {
                return group;
            }
        }

        for (Group group : scaleOutGroups) {
            if (group.getName().equalsIgnoreCase(groupName)) {
                return group;
            }
        }

        return null;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addSubMatrix(Matrix sub) {
        this.subMatrixs.add(sub);
    }

    public List<Matrix> getSubMatrixs() {
        return subMatrixs;
    }

    public void setSubMatrixs(List<Matrix> subMatrixs) {
        this.subMatrixs = subMatrixs;
    }

    public Map<String, RepoInst> getMasterRepoInstMap() {
        return masterRepoInstMap;
    }

    public void setMasterRepoInstMap(Map<String, RepoInst> masterRepoInstMap) {
        this.masterRepoInstMap = masterRepoInstMap;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}

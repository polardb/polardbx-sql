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

public class App {

    private String appName      = null;
    private String schemaFile   = null;
    private String ruleFile     = null;
    private String topologyFile = null;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getSchemaFile() {
        return schemaFile;
    }

    public void setSchemaFile(String schemaFile) {
        this.schemaFile = schemaFile;
    }

    public String getRuleFile() {
        return ruleFile;
    }

    public void setRuleFile(String ruleFile) {
        this.ruleFile = ruleFile;
    }

    public String getTopologyFile() {
        return topologyFile;
    }

    public void setTopologyFile(String topologyFile) {
        this.topologyFile = topologyFile;
    }

    @Override
    public String toString() {
        return "App [appName=" + appName + ", schemaFile=" + schemaFile + ", ruleFile=" + ruleFile + ", topologyFile="
            + topologyFile + "]";
    }

}

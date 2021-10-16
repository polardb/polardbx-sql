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

package com.alibaba.polardbx.optimizer.hint.operator;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * @author chenmo.cm
 */
public interface HintCmdOperator extends HintOperator {

    CmdBean handle(CmdBean current);

    public static class CmdBean {

        private String schemaName;
        private Map<String, Object> extraCmd = new HashMap<>();
        private StringBuilder groupHint = new StringBuilder();
        /**
         * for node()/scan() Hint
         */
        private boolean scan = false;
        private List<String> groups = new LinkedList<>();
        private String table = "";
        private String condition = "";
        private String json = "";
        private List<List<String>> realTable = new LinkedList<>();

        public CmdBean(String schemaName, Map<String, Object> extraCmd, String groupHint) {
            this.schemaName = schemaName;
            this.extraCmd = extraCmd;
            this.groupHint = new StringBuilder(groupHint == null ? "" : groupHint);
        }

        public String getSchemaName() {
            return schemaName;
        }

        public Map<String, Object> getExtraCmd() {
            return extraCmd;
        }

        public void setExtraCmd(Map<String, Object> extraCmd) {
            this.extraCmd = extraCmd;
        }

        public StringBuilder getGroupHint() {
            return groupHint;
        }

        public List<String> getGroups() {
            return groups;
        }

        public void setGroups(List<String> groups) {
            this.groups = groups;
        }

        public void setGroupHint(StringBuilder groupHint) {
            this.groupHint = groupHint;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getCondition() {
            return condition;
        }

        public void setCondition(String condition) {
            this.condition = condition;
        }

        public boolean doPushdown() {
            return this.isScan() || this.groups.size() > 0 || TStringUtil.isNotBlank(table) || jsonHint();
        }

        public boolean jsonHint() {
            return TStringUtil.isNotBlank(this.json);
        }

        public boolean isScan() {
            return scan;
        }

        public void setScan(boolean scan) {
            this.scan = scan;
        }

        public String getJson() {
            return json;
        }

        public void setJson(String json) {
            this.json = json;
        }

        public List<List<String>> getRealTable() {
            return realTable;
        }

        public void setRealTable(List<List<String>> realTable) {
            this.realTable = realTable;
        }

        public boolean logicalTableSpecified() {
            return TStringUtil.isNotBlank(getTable());
        }

        public boolean realTableSpecified() {
            return null != getRealTable() && getRealTable().size() > 0;
        }

        public boolean groupSpecified() {
            return null != getGroups() && getGroups().size() > 0;
        }
    }
}

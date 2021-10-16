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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 * @ClassName DrdsMoveDataBase
 * @description
 * @Author luoyanxin
 * @Date 2019-12-17 15:41
 */
public class DrdsMoveDataBase extends MySqlStatementImpl implements SQLStatement {

    private Map<String, List<String>> storageGroups = new HashMap();
    private boolean isCleanUpCommand = false;
    private static String VIRTUAL_KEY = "VIRTUAL_KEY";

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public Map<String, List<String>> getStorageGroups() {
        return storageGroups;
    }

    public void put(String storageId, List<String> groups) {
        storageGroups.put(storageId, groups);
    }

    public void put(List<String> groups) {
        assert isCleanUpCommand;
        storageGroups.put(VIRTUAL_KEY,groups);
    }

    public void put(String storageId, String group) {
        if (storageGroups.containsKey(storageId) ){
            storageGroups.get(storageId).add(group);
        } else {
            List<String> groups = new ArrayList<String>();
            groups.add(group);
            storageGroups.put(storageId, groups);
        }
    }

    public void setCleanUpCommand(boolean cleanUpCommand) {
        isCleanUpCommand = cleanUpCommand;
    }

    public boolean isCleanUpCommand() {
        return isCleanUpCommand;
    }
}

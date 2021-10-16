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

package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.ArrayList;
import java.util.List;

public class MultiDdlContext {

    private int numOfPlans = 1;
    private int planIndex = 0;
    private List<String> ddlStmts = null;

    public int getNumOfPlans() {
        return numOfPlans;
    }

    public void initPlanInfo(int numOfPlans, ExecutionContext executionContext) {
        this.numOfPlans = numOfPlans;
        this.planIndex = 0;
        extractDdlStmts(executionContext);
    }

    private void extractDdlStmts(ExecutionContext executionContext) {
        String origDdlStmts = executionContext.getOriginSql();
        if (numOfPlans > 1 && TStringUtil.isNotEmpty(origDdlStmts)) {
            String[] tmpDdlStmts = TStringUtil.split(origDdlStmts, ';');
            if (tmpDdlStmts != null && tmpDdlStmts.length > 1) {
                // Multiple statements
                ddlStmts = new ArrayList<>(numOfPlans);
                for (String ddlStmt : tmpDdlStmts) {
                    if (TStringUtil.isNotBlank(ddlStmt) && !TStringUtil.isWhitespace(ddlStmt)) {
                        ddlStmts.add(ddlStmt);
                    }
                }
            }
        }
    }

    public int getPlanIndex() {
        return planIndex;
    }

    public List<String> getDdlStmts() {
        return ddlStmts;
    }

    public void incrementPlanIndex() {
        this.planIndex++;
    }

}

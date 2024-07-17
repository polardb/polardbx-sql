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

import java.util.HashSet;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class LBACSecurityPolicy {

    private String policyName;

    private String[] componentNames;

    private Set<String> labelNames;

    public LBACSecurityPolicy(String policyName, String[] componentNames) {
        this.policyName = policyName;
        this.componentNames = componentNames;
        this.labelNames = new HashSet<>();
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }

    public String[] getComponentNames() {
        return componentNames;
    }

    public void setComponentNames(String[] componentNames) {
        this.componentNames = componentNames;
    }

    public void addLabel(String labelName) {
        this.labelNames.add(labelName);
    }

    public boolean containLabel(String labelName) {
        return this.labelNames.contains(labelName);
    }

    public Set<String> getLabelNames() {
        return labelNames;
    }
}

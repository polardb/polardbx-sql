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

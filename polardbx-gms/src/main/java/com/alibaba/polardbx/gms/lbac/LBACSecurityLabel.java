package com.alibaba.polardbx.gms.lbac;

import com.alibaba.polardbx.gms.lbac.component.ComponentInstance;

/**
 * @author pangzhaoxing
 */
public class LBACSecurityLabel {

    private String labelName;

    private String policyName;

    private ComponentInstance[] components;

    public LBACSecurityLabel(ComponentInstance[] components) {
        this.components = components;
    }

    public LBACSecurityLabel(String labelName, String policyName, ComponentInstance[] components) {
        this.labelName = labelName;
        this.policyName = policyName;
        this.components = components;
    }

    public ComponentInstance[] getComponents() {
        return components;
    }

    public void setComponents(ComponentInstance[] components) {
        this.components = components;
    }

    public String getLabelName() {
        return labelName;
    }

    public void setLabelName(String labelName) {
        this.labelName = labelName;
    }

    public String getPolicyName() {
        return policyName;
    }

    public void setPolicyName(String policyName) {
        this.policyName = policyName;
    }
}

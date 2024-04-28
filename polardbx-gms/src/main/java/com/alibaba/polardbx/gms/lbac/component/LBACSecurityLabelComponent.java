package com.alibaba.polardbx.gms.lbac.component;

import java.util.Set;

/**
 * @author pangzhaoxing
 */
public abstract class LBACSecurityLabelComponent {

    protected String componentName;
    protected ComponentType type;

    public LBACSecurityLabelComponent() {
    }

    public LBACSecurityLabelComponent(String componentName, ComponentType type) {
        this.componentName = componentName;
        this.type = type;
    }

    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public ComponentType getType() {
        return type;
    }

    public void setType(ComponentType type) {
        this.type = type;
    }

    abstract public Set<String> getAllTags();

    abstract public boolean containTag(String tag);

}

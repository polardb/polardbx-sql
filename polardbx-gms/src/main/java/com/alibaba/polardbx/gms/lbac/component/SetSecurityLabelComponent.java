package com.alibaba.polardbx.gms.lbac.component;

import java.util.HashSet;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class SetSecurityLabelComponent extends LBACSecurityLabelComponent {

    private final Set<String> tags = new HashSet<>();

    public SetSecurityLabelComponent(String componentName, Set<String> tags) {
        super(componentName, ComponentType.SET);
        this.tags.addAll(tags);
    }

    public SetSecurityLabelComponent(String componentName, String componentContent) {
        super(componentName, ComponentType.SET);
    }

    @Override
    public Set<String> getAllTags() {
        return tags;
    }

    @Override
    public boolean containTag(String tag) {
        return tags.contains(tag);
    }
}

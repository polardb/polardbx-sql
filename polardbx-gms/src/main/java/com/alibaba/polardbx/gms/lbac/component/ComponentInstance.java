package com.alibaba.polardbx.gms.lbac.component;

import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class ComponentInstance {

    private Set<String> tags;

    public ComponentInstance(Set<String> tags) {
        this.tags = tags;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }
}

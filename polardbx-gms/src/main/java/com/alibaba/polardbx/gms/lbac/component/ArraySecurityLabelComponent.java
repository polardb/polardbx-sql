package com.alibaba.polardbx.gms.lbac.component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class ArraySecurityLabelComponent extends LBACSecurityLabelComponent {

    protected final Map<String, Integer> tagIndexMap = new HashMap<>();

    public ArraySecurityLabelComponent(String componentName, List<String> tags) {
        super(componentName, ComponentType.ARRAY);
        for (int i = 0; i < tags.size(); i++) {
            if (tagIndexMap.put(tags.get(i), i) != null) {
                throw new IllegalArgumentException("the tags should not be same");
            }
        }
    }

    public ArraySecurityLabelComponent(String componentName, String componentContent) {
        super(componentName, ComponentType.SET);
    }

    @Override
    public Set<String> getAllTags() {
        return tagIndexMap.keySet();
    }

    /**
     * @param tag negative means tag is not in array
     */
    public int getTagIndex(String tag) {
        return tagIndexMap.getOrDefault(tag, -1);
    }

    @Override
    public boolean containTag(String tag) {
        return tagIndexMap.containsKey(tag);
    }
}

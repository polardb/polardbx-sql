package com.alibaba.polardbx.gms.lbac.component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class TreeSecurityLabelComponent extends LBACSecurityLabelComponent {

    private Node root;

    private final Map<String, Node> tagNodeMap = new HashMap<>();

    public TreeSecurityLabelComponent(String componentName, Node root) {
        super(componentName, ComponentType.TREE);
        this.root = root;
        collectTags(root);
    }

    public TreeSecurityLabelComponent(String componentName, String componentContent) {
        super(componentName, ComponentType.SET);
    }

    private void collectTags(Node node) {
        if (tagNodeMap.put(node.getTag(), node) != null) {
            throw new IllegalArgumentException("the tag should not be same");
        }
        for (Node child : node.getChildren()) {
            collectTags(child);
        }
    }

    @Override
    public Set<String> getAllTags() {
        return tagNodeMap.keySet();
    }

    public Node getNode(String tag) {
        return tagNodeMap.get(tag);
    }

    public Node getRoot() {
        return root;
    }

    @Override
    public boolean containTag(String tag) {
        return tagNodeMap.containsKey(tag);
    }

    public static class Node {
        private String tag;

        private Set<Node> children = new HashSet<>();

        public Node(String tag) {
            this.tag = tag;
        }

        public String getTag() {
            return tag;
        }

        public Set<Node> getChildren() {
            return children;
        }

        public void addChild(Node node) {
            this.children.add(node);
        }

        public static boolean isAncestor(Node node1, Node node2) {
            if (node1.children.contains(node2)) {
                return true;
            }
            for (Node child : node1.children) {
                if (isAncestor(child, node2)) {
                    return true;
                }
            }
            return false;
        }

    }
}

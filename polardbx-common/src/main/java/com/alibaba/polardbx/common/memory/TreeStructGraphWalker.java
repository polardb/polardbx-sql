package com.alibaba.polardbx.common.memory;

import com.google.common.base.Preconditions;
import org.openjdk.jol.info.GraphPathRecord;
import org.openjdk.jol.info.GraphVisitor;
import org.openjdk.jol.util.VMSupport;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TreeStructGraphWalker {

    private final Set<Object> visited;
    private final Object root;
    private final Collection<GraphVisitor> visitors;

    private TreeNode rootTreeNode;
    private Map<TreeNode, List<TreeNode>> memoryUsageTree = new HashMap<>();

    private static class TreeNode {
        final String objectIdentifier;
        final String fieldIdentifier;
        final long memorySize;

        TreeNode(String objectIdentifier, String fieldIdentifier, long memorySize) {
            this.objectIdentifier = objectIdentifier;
            this.fieldIdentifier = fieldIdentifier;
            this.memorySize = memorySize;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (object == null || getClass() != object.getClass()) {
                return false;
            }

            TreeNode treeNode = (TreeNode) object;
            return Objects.equals(objectIdentifier, treeNode.objectIdentifier) && Objects.equals(
                fieldIdentifier, treeNode.fieldIdentifier);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(objectIdentifier);
            result = 31 * result + Objects.hashCode(fieldIdentifier);
            return result;
        }

        static String objectIdentifier(Object o) {
            return o.getClass().getSimpleName() + "@" + System.identityHashCode(o);
        }

        static String fieldIdentifier(Field f) {
            return f.getName();
        }
    }

    public TreeStructGraphWalker(Object root) {
        this.root = root;
        this.visited = Collections.newSetFromMap(new IdentityHashMap<>());
        this.visitors = new ArrayList<>();
    }

    public void addVisitor(GraphVisitor v) {
        visitors.add(v);
    }

    public String walk() {
        List<GraphPathRecord> curLayer = new ArrayList<>();
        List<GraphPathRecord> newLayer = new ArrayList<>();

        GraphPathRecord e = new GraphPathRecord("root", root, 0);
        visited.add(root);
        visitObject(e);

        rootTreeNode = new TreeNode(
            TreeNode.objectIdentifier(e.obj()),
            e.path(),
            VMSupport.sizeOf(e.obj()));
        memoryUsageTree.put(rootTreeNode, new ArrayList<>());

        curLayer.add(e);

        while (!curLayer.isEmpty()) {
            newLayer.clear();
            for (GraphPathRecord next : curLayer) {

                List<GraphPathRecord> refs = peelReferences(next);

                List<TreeNode> treeNodeList = memoryUsageTree.get(new TreeNode(
                    TreeNode.objectIdentifier(next.obj()),
                    next.path(),
                    0L
                ));
                Preconditions.checkNotNull(treeNodeList);

                for (GraphPathRecord ref : refs) {
                    if (ref != null) {

                        if (visited.add(ref.obj())) {
                            TreeNode newTreeNode = new TreeNode(
                                TreeNode.objectIdentifier(ref.obj()),
                                ref.path(),
                                VMSupport.sizeOf(ref.obj())
                            );
                            treeNodeList.add(newTreeNode);
                            memoryUsageTree.put(newTreeNode, new ArrayList<>());

                            visitObject(ref);
                            newLayer.add(ref);
                        }
                    }
                }
            }
            curLayer.clear();
            curLayer.addAll(newLayer);
        }

        StringBuilder builder = new StringBuilder();

        long accumulatedMemorySize = printTreeNode(builder, rootTreeNode, 0);
        return builder.toString();
    }

    private long printTreeNode(StringBuilder builder, TreeNode treeNode, int depth) {
        List<TreeNode> treeNodeList = memoryUsageTree.get(treeNode);
        // print
        for (int i = 0; i < depth; i++) {
            builder.append(' ');
        }
        builder.append("└ ");
        builder.append(treeNode.objectIdentifier);
        builder.append("__");
        builder.append(treeNode.fieldIdentifier);
        builder.append(": ");
        builder.append(treeNode.memorySize);
        builder.append(" bytes\n");


        long accumulatedMemorySize = treeNode.memorySize;
        for (TreeNode childTreeNode : treeNodeList) {
            List<TreeNode> childTreeNodeList = memoryUsageTree.get(childTreeNode);
            if (childTreeNodeList != null) {
                accumulatedMemorySize += printTreeNode(builder, childTreeNode, depth + 1);
            }
        }

//        builder.append(", accumulated = ");
//        builder.append(accumulatedMemorySize);
//        builder.append(" bytes");
//        builder.append("\n");

        return accumulatedMemorySize;
    }

    private void visitObject(GraphPathRecord record) {
        for (GraphVisitor v : visitors) {
            v.visit(record);
        }
    }

    private List<GraphPathRecord> peelReferences(GraphPathRecord r) {
        List<GraphPathRecord> result = new ArrayList<GraphPathRecord>();

        Object o = r.obj();
        if (o.getClass().isArray() && !o.getClass().getComponentType().isPrimitive()) {
            int c = 0;
            for (Object e : (Object[]) o) {
                if (e != null) {
                    result.add(new GraphPathRecord(r.path() + "[" + c + "]", e, r.depth() + 1));
                }
                c++;
            }
        }

        for (Field f : getAllFields(o.getClass())) {
            f.setAccessible(true);
            if (f.getType().isPrimitive()) {
                continue;
            }
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }

            try {
                Object e = f.get(o);
                if (e != null) {
                    result.add(new GraphPathRecord(f.getName(), e, r.depth() + 1));
                }
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        return result;
    }

    private Collection<Field> getAllFields(Class<?> klass) {
        List<Field> results = new ArrayList<Field>();

        for (Field f : klass.getDeclaredFields()) {
            if (!Modifier.isStatic(f.getModifiers())) {
                results.add(f);
            }
        }

        Class<?> superKlass = klass;
        while ((superKlass = superKlass.getSuperclass()) != null) {
            for (Field f : superKlass.getDeclaredFields()) {
                if (!Modifier.isStatic(f.getModifiers())) {
                    results.add(f);
                }
            }
        }

        return results;
    }
}

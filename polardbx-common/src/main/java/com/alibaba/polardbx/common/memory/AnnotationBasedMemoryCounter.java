package com.alibaba.polardbx.common.memory;

import com.google.common.base.Preconditions;
import org.openjdk.jol.util.VMSupport;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AnnotationBasedMemoryCounter implements FastMemoryCounter {

    private final int maxDepth;
    private final boolean useFieldMemoryCounter;
    private final boolean generateFieldSizeMap;
    private final boolean generateTreeStruct;

    private static ThreadLocal<Set<String>> threadLocalVisitedAddressSet =
        ThreadLocal.withInitial(() -> new HashSet<>());

    private Map<String, Integer> fieldSizeMap = null;
    private long totalSize;

    private TreeNode rootTreeNode;
    private Map<TreeNode, List<TreeNode>> memoryUsageTree = new HashMap<>();

    private static class TreeNode {
        final String fieldIdentifier;
        final long memorySize;
        boolean fieldMemoryVisible;

        TreeNode(String fieldIdentifier, long memorySize) {
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
            return Objects.equals(fieldIdentifier, treeNode.fieldIdentifier);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fieldIdentifier);
        }

        static String objectIdentifier(Object o) {
            return o.getClass().getSimpleName() + "@" + System.identityHashCode(o);
        }

        static String fieldIdentifier(Field f) {
            return f.getName();
        }
    }

    public AnnotationBasedMemoryCounter() {
        this(DEFAULT_MAX_DEPTH, true, false, false);
    }

    public AnnotationBasedMemoryCounter(int maxDepth, boolean useFieldMemoryCounter, boolean generateFieldSizeMap,
                                        boolean generateTreeStruct) {
        this.maxDepth = maxDepth;
        this.useFieldMemoryCounter = useFieldMemoryCounter;
        this.generateFieldSizeMap = generateFieldSizeMap;
        this.generateTreeStruct = generateTreeStruct;
    }

    private static class VisitObject {
        final Object reference;
        final int depth;
        final int identityHash;

        String identifier;
        boolean fieldMemoryVisible = false;

        private VisitObject(Object object, int depth) {
            this.reference = object;
            this.depth = depth;
            this.identityHash = System.identityHashCode(object);
        }

        @Override
        public int hashCode() {
            // Identity hash code of the original key
            return identityHash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof VisitObject) {
                Object key = reference;
                Object otherKey = ((VisitObject) obj).reference;
                return key != null
                    && otherKey != null
                    && key == otherKey; // Reference equality
            }
            return false;
        }

        @Override
        public String toString() {
            return "VisitObject{" +
                "reference=" + reference +
                ", depth=" + depth +
                ", identityHash=" + identityHash +
                ", identifier='" + identifier + '\'' +
                '}';
        }
    }

    @Override
    public MemoryUsageReport getMemoryUsage(Object root) {
        Set<String> visitedAddressSet = threadLocalVisitedAddressSet.get();

        try {
            long rootAddress = VMSupport.addressOf(root);
            int rootHC = System.identityHashCode(root);
            String name = root.getClass().getSimpleName()
                + "@" + System.identityHashCode(root);

            if (generateFieldSizeMap) {
                fieldSizeMap = new HashMap<>();
            }

            List<VisitObject> curLayer = new ArrayList<>();
            List<VisitObject> newLayer = new ArrayList<>();

            VisitObject e = new VisitObject(root, 0);
            e.identifier = name;
            visitedAddressSet.add(identifier(e.reference));
            accumulateMemoryUsage(e);

            rootTreeNode = new TreeNode(
                e.identifier,
                VMSupport.sizeOf(e.reference)
            );
            memoryUsageTree.put(rootTreeNode, new ArrayList<>());

            curLayer.add(e);

            while (!curLayer.isEmpty()) {
                newLayer.clear();
                for (VisitObject next : curLayer) {

                    List<VisitObject> refInThisLayer = handleReferences(next);

                    List<TreeNode> treeNodeList = memoryUsageTree.get(
                        new TreeNode(
                            next.identifier,
                            0L
                        )
                    );
                    Preconditions.checkNotNull(treeNodeList);

                    for (VisitObject ref : refInThisLayer) {

                        if (ref != null) {
                            if (visitedAddressSet.add(identifier(ref.reference))) {

                                accumulateMemoryUsage(ref);

                                TreeNode newTreeNode = new TreeNode(
                                    ref.identifier, VMSupport.sizeOf(ref.reference)
                                );
                                newTreeNode.fieldMemoryVisible = ref.fieldMemoryVisible;
                                treeNodeList.add(newTreeNode);
                                memoryUsageTree.put(newTreeNode, new ArrayList<>());

                                // Don't exceed the maximum depth.
                                if (ref.depth <= maxDepth) {
                                    newLayer.add(ref);
                                }

                            }
                        }
                    }
                }
                curLayer.clear();
                curLayer.addAll(newLayer);
            }

            String treeStruct = "";
            if (generateTreeStruct) {

                StringBuilder treeNodeStringBuilder =
                    new StringBuilder().append("Total usage : ").append(totalSize).append(" bytes\n");
                printTreeNode(treeNodeStringBuilder, rootTreeNode, 0);

                treeStruct = treeNodeStringBuilder.toString();
            }

            return new MemoryUsageReport(fieldSizeMap, totalSize, treeStruct);
        } finally {

            // clear thread local collections.
            visitedAddressSet.clear();
        }

    }

    private long printTreeNode(StringBuilder builder, TreeNode treeNode, int depth) {
        List<TreeNode> treeNodeList = memoryUsageTree.get(treeNode);
        // print
        for (int i = 0; i < 2 * depth; i++) {
            if (i % 2 == 0) {
                builder.append('|');
            } else {
                builder.append(' ');
            }
        }
        builder.append("└ ");
        // builder.append("——");
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

        if (treeNode.fieldMemoryVisible) {
            fieldSizeMap.put(treeNode.fieldIdentifier, (int) accumulatedMemorySize);
        }

        return accumulatedMemorySize;
    }

    private void accumulateMemoryUsage(VisitObject visitObject) {
        final String fieldIdentifier = visitObject.identifier;
        try {
            Object object = visitObject.reference;
            final int size = VMSupport.sizeOf(object);
            totalSize += size;

            if (generateFieldSizeMap && fieldIdentifier != null && !fieldIdentifier.isEmpty()) {
                fieldSizeMap.put(fieldIdentifier, size);
            }
        } catch (Exception e) {
            if (generateFieldSizeMap && fieldIdentifier != null && !fieldIdentifier.isEmpty()) {
                fieldSizeMap.put(fieldIdentifier, -1);
            }
        }

    }

    private List<VisitObject> handleReferences(VisitObject r) {
        List<VisitObject> result = new ArrayList<>();

        Object o = r.reference;

        if (o.getClass().isArray() && !o.getClass().getComponentType().isPrimitive()) {
            int c = 0;
            for (Object e : (Object[]) o) {
                if (e != null) {

                    VisitObject newVisitObject = new VisitObject(e, r.depth + 1);
                    newVisitObject.identifier = "[" + c + "]__" + identifier(e);
                    result.add(newVisitObject);

                    c++;
                }
            }
        }

        for (Field f : getAllFields(o.getClass(), useFieldMemoryCounter)) {
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
                    VisitObject visitObject = new VisitObject(e, r.depth + 1);

                    // identifier for field with annotation @FieldMemoryVisible
                    visitObject.identifier = e.getClass().getSimpleName()
                        + "@" + System.identityHashCode(e)
                        + "__" + f.getName();

                    visitObject.fieldMemoryVisible = f.isAnnotationPresent(FieldMemoryVisible.class);

                    result.add(visitObject);
                }
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        return result;
    }

    private static Collection<Field> getAllFields(Class<?> klass, boolean useFieldMemoryCounter) {
        List<Field> results = new ArrayList<>();

        for (Field f : klass.getDeclaredFields()) {
            // Ignore static field and check field annotation.
            if (!Modifier.isStatic(f.getModifiers())
                && (!useFieldMemoryCounter || checkAnnotation(f))) {
                results.add(f);
            }
        }

        Class<?> superKlass = klass;
        while ((superKlass = superKlass.getSuperclass()) != null) {
            for (Field f : superKlass.getDeclaredFields()) {
                // For supper class,
                // ignore static field and check field annotation.
                if (!Modifier.isStatic(f.getModifiers())
                    && (!useFieldMemoryCounter || checkAnnotation(f))) {
                    results.add(f);
                }
            }
        }

        return results;
    }

    private static boolean checkAnnotation(Field field) {
        // check annotation @FieldMemoryCounter
        FieldMemoryCounter annotation = field.getAnnotation(FieldMemoryCounter.class);
        if (annotation != null) {
            return annotation.value();
        }
        // no annotation, return true in default.
        return true;
    }

    private static String identifier(Object object) {
        return object.getClass().getSimpleName() + "@" + System.identityHashCode(object);
    }

}

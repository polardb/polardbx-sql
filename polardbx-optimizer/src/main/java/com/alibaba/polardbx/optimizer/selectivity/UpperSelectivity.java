package com.alibaba.polardbx.optimizer.selectivity;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class UpperSelectivity {

    private enum SelectivityKind {
        FREE, BOUND, UNKNOWN
    }

    final SelectivityKind kind;
    final double selectivity;
    final int idx;

    private UpperSelectivity(SelectivityKind kind, double selectivity) {
        this.kind = kind;
        this.selectivity = selectivity;
        this.idx = -1;
    }

    private UpperSelectivity(SelectivityKind kind, double selectivity, int idx) {
        this.kind = kind;
        this.selectivity = selectivity;
        this.idx = idx;
    }

    public double getSelectivity() {
        return selectivity;
    }

    public int getIdx() {
        return idx;
    }

    public static final UpperSelectivity FREE = new UpperSelectivity(SelectivityKind.FREE, 1D);

    public static final UpperSelectivity UNKNOWN = new UpperSelectivity(SelectivityKind.UNKNOWN, 0D);

    public static UpperSelectivity createBound(double selectivity) {
        if (selectivity < 0) {
            selectivity = 0D;
        }
        if (selectivity > 1) {
            selectivity = 1D;
        }
        return new UpperSelectivity(SelectivityKind.BOUND, selectivity);
    }

    public static UpperSelectivity createBound(double selectivity, int idx) {
        if (selectivity < 0) {
            selectivity = 0D;
        }
        if (selectivity > 1) {
            selectivity = 1D;
        }
        return new UpperSelectivity(SelectivityKind.BOUND, selectivity, idx);
    }

    public static UpperSelectivity not(UpperSelectivity pred) {
        if (pred == UNKNOWN) {
            return pred;
        }
        if (pred == FREE) {
            return pred;
        }
        return FREE;
    }

    public static UpperSelectivity or(Collection<UpperSelectivity> disconj) {
        if (CollectionUtils.isEmpty(disconj)) {
            return createBound(1D);
        }
        Map<SelectivityKind, List<UpperSelectivity>> map = classifyKind(disconj);
        if (!CollectionUtils.isEmpty(map.get(SelectivityKind.FREE))) {
            return FREE;
        }
        if (!CollectionUtils.isEmpty(map.get(SelectivityKind.BOUND))) {
            double max = 0D;
            for (UpperSelectivity sel : map.get(SelectivityKind.BOUND)) {
                max += sel.selectivity;
            }
            return createBound(max);
        }
        return UNKNOWN;
    }

    public static UpperSelectivity and(Collection<UpperSelectivity> conj) {
        if (CollectionUtils.isEmpty(conj)) {
            return createBound(1D);
        }
        Map<SelectivityKind, List<UpperSelectivity>> map = classifyKind(conj);

        if (!CollectionUtils.isEmpty(map.get(SelectivityKind.BOUND))) {
            List<UpperSelectivity> upperSelectivities = map.get(SelectivityKind.BOUND);
            upperSelectivities.sort(Comparator.comparingDouble(x -> x.selectivity));
            double result = 1;
            for (int i = 0; i < Math.min(upperSelectivities.size(), 4); i++) {
                result *= Math.pow(upperSelectivities.get(i).selectivity, 1D / Math.pow(2, i));
            }
            return createBound(result);
        }
        if (CollectionUtils.isEmpty(map.get(SelectivityKind.FREE))) {
            return UNKNOWN;
        }
        return FREE;
    }

    public static Map<SelectivityKind, List<UpperSelectivity>> classifyKind(Collection<UpperSelectivity> conj) {
        Map<SelectivityKind, List<UpperSelectivity>> map =
            ImmutableMap.<SelectivityKind, List<UpperSelectivity>>builder()
                .put(SelectivityKind.FREE, Lists.newArrayList())
                .put(SelectivityKind.BOUND, Lists.newArrayList())
                .put(SelectivityKind.UNKNOWN, Lists.newArrayList())
                .build();
        for (UpperSelectivity sel : conj) {
            map.get(sel.kind).add(sel);
        }
        return map;
    }
}

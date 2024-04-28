package org.apache.calcite.rel;

import org.apache.calcite.plan.RelMultipleTrait;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.jetbrains.annotations.NotNull;

public class RelPartitionWises {

    public static RelPartitionWise ANY = new RelPartitionWiseImpl(false, false);

    public static RelPartitionWise LOCAL = new RelPartitionWiseImpl(true, false);

    public static RelPartitionWise REMOTE = new RelPartitionWiseImpl(false, true);

    public static RelPartitionWise ALL = new RelPartitionWiseImpl(true, true);

    public static class RelPartitionWiseImpl implements RelPartitionWise {
        private static final int localPartitionMask = 1 << 1;

        private static final int remotePartitionMask = 1;

        private final int code;

        public RelPartitionWiseImpl(boolean localPartition, boolean remotePartition) {
            this.code = (localPartition ? localPartitionMask : 0)
                + (remotePartition ? remotePartitionMask : 0);
        }

        @Override
        public boolean isTop() {
            return !(isLocalPartition() || isRemotePartition());
        }

        @Override
        public RelTraitDef getTraitDef() {
            return RelPartitionWiseTraitDef.INSTANCE;
        }

        @Override
        public boolean satisfies(RelTrait trait) {
            if (!(trait instanceof RelPartitionWise)) {
                return false;
            }
            RelPartitionWise rel = (RelPartitionWise) trait;
            return (isLocalPartition() == rel.isLocalPartition()) && (isRemotePartition() == rel.isRemotePartition());
        }

        @Override
        public void register(RelOptPlanner planner) {
        }

        @Override
        public boolean isLocalPartition() {
            return (code & localPartitionMask) > 0;
        }

        @Override
        public boolean isRemotePartition() {
            return (code & remotePartitionMask) > 0;
        }

        @Override
        public int compareTo(@NotNull RelMultipleTrait o) {
            RelPartitionWise partitionWise = (RelPartitionWise) o;
            return Integer.compare(getCode(), partitionWise.getCode());
        }

        @Override
        public int getCode() {
            return code;
        }

        @Override
        public int hashCode() {
            return code;
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj
                || obj instanceof RelPartitionWises.RelPartitionWiseImpl
                && code == ((RelPartitionWises.RelPartitionWiseImpl) obj).code;
        }

        @Override
        public String toString() {
            switch (code) {
            case localPartitionMask:
                return "[local]";
            case remotePartitionMask:
                return "[remote]";
            case localPartitionMask + remotePartitionMask:
                return "[local, remote]";
            default:
                return "[]";
            }
        }
    }
}

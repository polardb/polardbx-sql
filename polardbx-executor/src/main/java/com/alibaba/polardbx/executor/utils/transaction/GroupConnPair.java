package com.alibaba.polardbx.executor.utils.transaction;

import java.util.Objects;

/**
 * @author dylan
 * <p>
 * wuzhe move this class from InformationSchemaInnodbTrxHandler to here
 * <p>
 * A (group, DN connection id) pair, which can represent a unique transaction
 */
public class GroupConnPair {
    final private String group;
    final private long connId;

    public GroupConnPair(String group, long connId) {
        this.group = group;
        this.connId = connId;
    }

    public String getGroup() {
        return group;
    }

    public long getConnId() {
        return connId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GroupConnPair that = (GroupConnPair) o;
        return connId == that.connId &&
            group.equals(that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group, connId);
    }
}

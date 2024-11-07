package com.alibaba.polardbx.gms.ttl;

import com.alibaba.polardbx.druid.util.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public enum TtlPartArcState {

    /**
     * means part data does no change from reusing part and to be redo archiving,
     * the part having an archiving file, so not allow splitting part
     */
    ARC_STATE_REREADY(6, "REREADY"),

    /**
     * means part has been finished archiving at lease once, now is allowed inserting,  not allow splitting part
     */
    ARC_STATE_REUSING(5, "REUSING"),

    /**
     * means part finished archiving, part data is truncated, not allow splitting part
     */
    ARC_STATE_ARCHIVED(4, "ARCHIVED"),

    /**
     * means part is running archiving, building archiving files, not allow splitting part
     */
    ARC_STATE_ARCHIVING(3, "ARCHIVING"),

    /**
     * means part data does no change, has no archiving file, allow splitting part
     */
    ARC_STATE_READY(2, "READY"),

    /**
     * means part has incremental data, allow splitting part
     */
    ARC_STATE_USING(1, "USING"),

    /**
     * means part must be empty, allow splitting part
     */
    ARC_STATE_NO_USE(0, "NO_USE");

    protected int arcState;
    protected String arcStateName;

    TtlPartArcState(int arcState, String arcStateName) {
        this.arcState = arcState;
        this.arcStateName = arcStateName;
    }

    public int getArcState() {
        return arcState;
    }

    public String getArcStateName() {
        return arcStateName;
    }

    public static TtlPartArcState getTtlPartArcStateByArcStateValue(int arcState) {
        switch (arcState) {
        case 0:
            return TtlPartArcState.ARC_STATE_NO_USE;
        case 1:
            return TtlPartArcState.ARC_STATE_USING;
        case 2:
            return TtlPartArcState.ARC_STATE_READY;
        case 3:
            return TtlPartArcState.ARC_STATE_ARCHIVING;
        case 4:
            return TtlPartArcState.ARC_STATE_ARCHIVED;
        case 5:
            return TtlPartArcState.ARC_STATE_REUSING;
        case 6:
            return TtlPartArcState.ARC_STATE_REREADY;
        default:
            return TtlPartArcState.ARC_STATE_NO_USE;
        }
    }

    public static List<String> toTtlArcStateNames(Collection<TtlPartArcState> ttlPartArcStateList) {
        List<String> names = new ArrayList<>();
        for (TtlPartArcState state : ttlPartArcStateList) {
            names.add(state.getArcStateName());
        }
        return names;
    }

    public static Set<TtlPartArcState> ARC_STATE_SET_FOR_PERFORM_ARCHIVING = new HashSet<>();

    static {
        ARC_STATE_SET_FOR_PERFORM_ARCHIVING.add(TtlPartArcState.ARC_STATE_ARCHIVING);
        ARC_STATE_SET_FOR_PERFORM_ARCHIVING.add(TtlPartArcState.ARC_STATE_ARCHIVED);
    }

    public static Set<TtlPartArcState> ARC_STATE_SET_ALLOWED_TO_CONVERT_TO_USING = new HashSet<>();

    static {
        ARC_STATE_SET_ALLOWED_TO_CONVERT_TO_USING.add(TtlPartArcState.ARC_STATE_ARCHIVED);
        ARC_STATE_SET_ALLOWED_TO_CONVERT_TO_USING.add(TtlPartArcState.ARC_STATE_READY);
        ARC_STATE_SET_ALLOWED_TO_CONVERT_TO_USING.add(TtlPartArcState.ARC_STATE_REREADY);
    }

    public static TtlPartArcState getTtlPartArcStateByArcStateName(String arcStateName) {

        if (StringUtils.isEmpty(arcStateName)) {
            return TtlPartArcState.ARC_STATE_NO_USE;
        }

        if (TtlPartArcState.ARC_STATE_NO_USE.getArcStateName().equalsIgnoreCase(arcStateName)) {
            return TtlPartArcState.ARC_STATE_NO_USE;
        }

        if (TtlPartArcState.ARC_STATE_USING.getArcStateName().equalsIgnoreCase(arcStateName)) {
            return TtlPartArcState.ARC_STATE_USING;
        }

        if (TtlPartArcState.ARC_STATE_READY.getArcStateName().equalsIgnoreCase(arcStateName)) {
            return TtlPartArcState.ARC_STATE_READY;
        }

        if (TtlPartArcState.ARC_STATE_ARCHIVING.getArcStateName().equalsIgnoreCase(arcStateName)) {
            return TtlPartArcState.ARC_STATE_ARCHIVING;
        }

        if (TtlPartArcState.ARC_STATE_ARCHIVED.getArcStateName().equalsIgnoreCase(arcStateName)) {
            return TtlPartArcState.ARC_STATE_ARCHIVED;
        }

        if (TtlPartArcState.ARC_STATE_REUSING.getArcStateName().equalsIgnoreCase(arcStateName)) {
            return TtlPartArcState.ARC_STATE_REUSING;
        }

        return TtlPartArcState.ARC_STATE_NO_USE;
    }
}

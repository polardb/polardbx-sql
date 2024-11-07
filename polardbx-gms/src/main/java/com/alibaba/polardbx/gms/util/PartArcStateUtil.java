package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.ttl.TtlPartArcState;

public class PartArcStateUtil {

    public String getArcStateNameFromExtraJson(ExtraFieldJSON partExtras) {

//        // means part has been finished archiving at lease once, now is allowed inserting,  not allow splitting part
//        public final static int ARC_STATE_REUSING = 5;
//        // means part finished archiving, part data is truncated, not allow splitting part
//        public final static int ARC_STATE_ARCHIVED = 4;
//        // means part is running archiving, building archiving files, not allow splitting part
//        public final static int ARC_STATE_ARCHIVING = 3;
//        // means part data does no change, has no archiving file, allow splitting part
//        public final static int ARC_STATE_READY = 2;
//        // means part has incremental data, allow splitting part
//        public final static int ARC_STATE_USING = 1;
//        // means part must be empty, allow splitting part
//        public final static int ARC_STATE_NO_USE = 0;

        int arcStateVal = partExtras.getArcState();
        TtlPartArcState arcState = TtlPartArcState.getTtlPartArcStateByArcStateValue(arcStateVal);
        return arcState.getArcStateName();
    }
}

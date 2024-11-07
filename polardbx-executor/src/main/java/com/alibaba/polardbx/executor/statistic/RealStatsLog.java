package com.alibaba.polardbx.executor.statistic;

import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;

/**
 * @author fangwu
 */
public class RealStatsLog {
    /**
     * real-time information
     */
    public static String statLog() {
        StringBuilder sb = new StringBuilder();
        for (RealStatsLogType type : RealStatsLogType.values()) {
            try {
                sb.append(type.name()).append(":").append(type.getLog()).append(",");
            } catch (Exception e) {
                ModuleLogInfo.getInstance().logError(Module.METRIC, type.name(), e);
            }
        }
        return sb.toString();
    }

}

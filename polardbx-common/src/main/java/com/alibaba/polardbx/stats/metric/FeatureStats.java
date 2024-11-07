package com.alibaba.polardbx.stats.metric;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.util.Collection;

/**
 * @author fangwu
 */
public class FeatureStats {

    protected static final Logger logger = LoggerFactory.getLogger(FeatureStats.class);

    private long[] longStats;

    private long sign;

    // common method
    public static String serialize(FeatureStats fs) {
        return JSON.toJSONString(fs);
    }

    public static FeatureStats deserialize(String jsonStr) {
        return JSON.parseObject(jsonStr, FeatureStats.class);
    }

    public String log() {
        StringBuilder stringBuilder = new StringBuilder();
        for (FeatureStatsItem item : FeatureStatsItem.values()) {
            stringBuilder.append(item.name()).append(":").append(longStats[item.ordinal()]).append(",");
        }
        // remove the last ","
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    public static FeatureStats merge(FeatureStats target, Collection<FeatureStats> source) {
        for (FeatureStats featureStats : source) {
            if (target.sign != featureStats.sign) {
                // warning log
                continue;
            }
            for (int i = 0; i < target.longStats.length; i++) {
                target.longStats[i] += featureStats.longStats[i];
            }
        }
        return target;
    }

    private FeatureStats() {
        // build stats
        int longStatsSize = FeatureStatsItem.longStatsSize();
        longStats = new long[longStatsSize];
        this.sign = FeatureStatsItem.classVersion();
    }

    public static FeatureStats INSTANCE = new FeatureStats();

    public static FeatureStats getInstance() {
        return INSTANCE;
    }

    /**
     * build a new instance
     */
    public static FeatureStats build() {
        return new FeatureStats();
    }

    public static void reset() {
        INSTANCE = build();
    }

    public void increment(FeatureStatsItem item) {
        longStats[item.ordinal()]++;
    }

    public void plus(FeatureStatsItem item, long num) {
        longStats[item.ordinal()] += num;
    }

    public long getLong(FeatureStatsItem item) {
        return longStats[item.ordinal()];
    }

    public long[] getLongStats() {
        return longStats;
    }

    public long getSign() {
        return sign;
    }

    public void setLongStats(long[] longStats) {
        this.longStats = longStats;
    }

    public void setSign(long sign) {
        this.sign = sign;
    }
}

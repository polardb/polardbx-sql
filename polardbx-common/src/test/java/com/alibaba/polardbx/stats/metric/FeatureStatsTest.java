package com.alibaba.polardbx.stats.metric;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static com.alibaba.polardbx.stats.metric.FeatureStats.deserialize;
import static com.alibaba.polardbx.stats.metric.FeatureStats.serialize;

/**
 * @author fangwu
 */
public class FeatureStatsTest {
    @Test
    public void testSerialize() {
        FeatureStats fs = FeatureStats.getInstance();
        fs.plus(FeatureStatsItem.FIX_PLAN_NUM, 10);
        fs.plus(FeatureStatsItem.NEW_BASELINE_NUM, 101);
        String jsonStr = serialize(fs);
        FeatureStats featureStats = deserialize(jsonStr);

        assert featureStats.getLong(FeatureStatsItem.FIX_PLAN_NUM) == 10;
        assert featureStats.getLong(FeatureStatsItem.NEW_BASELINE_NUM) == 101;
        System.out.println(fs.log());
        System.out.println(featureStats.log());
        assert fs.log().equalsIgnoreCase(featureStats.log());
    }

    @Test
    public void testMerge() {
        FeatureStats fs = FeatureStats.build();
        FeatureStats fs1 = FeatureStats.build();
        fs1.plus(FeatureStatsItem.FIX_PLAN_NUM, 10);
        fs1.plus(FeatureStatsItem.NEW_BASELINE_NUM, 101);

        FeatureStats fs2 = FeatureStats.build();
        fs2.plus(FeatureStatsItem.FIX_PLAN_NUM, 11);
        fs2.plus(FeatureStatsItem.NEW_BASELINE_NUM, 101);

        FeatureStats fs3 = FeatureStats.build();
        fs3.plus(FeatureStatsItem.FIX_PLAN_NUM, 13);
        fs3.plus(FeatureStatsItem.NEW_BASELINE_NUM, 102);

        FeatureStats.merge(fs, ImmutableList.of(fs1, fs2, fs3));
        assert fs.getLong(FeatureStatsItem.FIX_PLAN_NUM) == 34;
        assert fs.getLong(FeatureStatsItem.NEW_BASELINE_NUM) == 304;
    }
}

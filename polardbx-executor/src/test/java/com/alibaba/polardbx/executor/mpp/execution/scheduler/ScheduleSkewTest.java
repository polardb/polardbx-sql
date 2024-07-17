package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.common.utils.Assert;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ScheduleSkewTest {
    private static final int TEST_ROUND = 100000;

    private static final double EXPECT_SKEW_RATIO = 0.01;

    @Test
    public void testColumnarSchedule() {
        int skewCount = 0;
        int fileNum = RandomUtils.nextInt(32, 64);
        int nodes = RandomUtils.nextInt(3, 5);
        for (int i = 0; i < TEST_ROUND; ++i) {
            List<List<String>> orcFiles = getOrcFiles(fileNum);
            Map<Integer, Integer> balancedAssign = new HashMap<>();
            orcFiles.forEach(orcFile -> ColumnarNodeSelector.chooseBucketByTwoChoice(orcFile, nodes, balancedAssign));
            boolean skewed = checkResultSkew(balancedAssign);
            if (skewed) {
                skewCount++;
            }
        }

        if (skewCount > EXPECT_SKEW_RATIO * TEST_ROUND) {
            Assert.fail(String.format(
                "schedule skewed, skewed ratio is %s, larger than expect %s, file num is %s, and node size is %s",
                (double) skewCount / TEST_ROUND,
                EXPECT_SKEW_RATIO, fileNum, nodes));
        }
    }

    private boolean checkResultSkew(Map<Integer, Integer> balancedAssign) {
        int maxCount = balancedAssign.values().stream().max(Integer::compareTo).get();
        int minCount = balancedAssign.values().stream().min(Integer::compareTo).get();
        return maxCount > 2 * minCount;
    }

    private List<List<String>> getOrcFiles(int fileNum) {
        String name = "tpch-%s.orc";
        return IntStream.range(0, fileNum).mapToObj(t -> Arrays.asList(String.format(name, UUID.randomUUID())))
            .collect(Collectors.toList());
    }
}

package com.alibaba.polardbx.qatest.statistic.collect;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.util.DiffRepository;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import com.google.common.util.concurrent.AtomicDouble;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

/**
 * give accuracy judge data for statistic collection test
 * judge if test passed
 *
 * @author fangwu
 */
public class AccuracyQuantifier {
    private static AccuracyQuantifier accuracyQuantifier = new AccuracyQuantifier();

    private AccuracyQuantifier() {
    }

    public static AccuracyQuantifier getInstance() {
        return accuracyQuantifier;
    }

    public static final int FREQUENCY_SIZE = 100;
    public static final String configFile = "/repo/statisticTestBaseline.yml";
    public static final boolean baselineMode = false;

    /**
     * current statistic target indicators
     */
    enum QUANTIFIER_TYPE {
        ROWCOUNT,
        NDV,
        NULL_COUNT,
        FREQUENCY_EQUAL,
        FREQUENCY_RANGE;

        QUANTIFIER_TYPE() {
        }

        /**
         * uplimit name : 5W, 500000, 2000000
         */
        Map<String, AccuracyReport> accuracyReportMap = Maps.newConcurrentMap();

        public void feed(String upLimitName, long estimate, long real) {
            AccuracyReport ar = accuracyReportMap.computeIfAbsent(upLimitName, s -> new AccuracyReport());
            ar.feed(estimate, real);
        }

        public String judge(String upLimitName) throws IOException {
            if (StringUtils.isEmpty(upLimitName) ||
                "DefaultStatisticCollectionTest".equalsIgnoreCase(upLimitName)) {
                return "";
            }
            AccuracyReport accuracyReport = accuracyReportMap.get(upLimitName);
            if (accuracyReport == null) {
                return "";
            }
            StringBuilder errorReport = new StringBuilder();
            // judge
            Double[] upLimit = DiffRepository.getDoubleArray(configFile, upLimitName + "_" + name());
            if (upLimit == null) {
                upLimit = new Double[5];
                Double[] finalUpLimit = upLimit;
                IntStream.range(0, 5).forEach(i -> finalUpLimit[i] = 0D);
            }
            boolean updated = false;
            long testTimes = upLimit[0].longValue();
            double maxSumOfDiff = upLimit[1];
            double maxDiffSD = upLimit[2];
            double maxSumOfPercentage = upLimit[3];
            double maxPercentageSD = upLimit[4];

            long feedCount = accuracyReport.feedCount();
            if (feedCount != testTimes) {
                if (baselineMode) {
                    upLimit[0] = Double.valueOf(feedCount);
                    updated = true;
                }
                errorReport.append("feed count miss match, actual=" + feedCount + ", expected=" + testTimes);
                errorReport.append("\n");
            }

            long sumOfDiff = accuracyReport.sumOfDifferences();
            if (sumOfDiff > maxSumOfDiff) {
                if (baselineMode) {
                    upLimit[1] = Double.valueOf(sumOfDiff);
                    updated = true;
                }
                errorReport.append("sum of diff beyond bound, actual=" + sumOfDiff + ", upperbound=" + maxSumOfDiff);
                errorReport.append("\n");
            }

            // SD: Standard Deviation
            double diffSD = accuracyReport.diffSD();
            if (diffSD > maxDiffSD) {
                if (baselineMode) {
                    upLimit[2] = Double.valueOf(diffSD);
                    updated = true;
                }
                errorReport.append("diff SD beyond bound, actual=" + diffSD + ", upperbound=" + maxDiffSD);
                errorReport.append("\n");
            }

            double sumOfPercentage = accuracyReport.sumOfPercentage();
            if (sumOfPercentage > maxSumOfPercentage) {
                if (baselineMode) {
                    upLimit[3] = Double.valueOf(sumOfPercentage);
                    updated = true;
                }
                errorReport.append(
                    "sum of percentage beyond bound, actual=" + sumOfPercentage + ", upperbound=" + maxSumOfPercentage);
                errorReport.append("\n");
            }

            double percentageSD = accuracyReport.percentageSD();
            if (percentageSD > maxPercentageSD) {
                if (baselineMode) {
                    upLimit[4] = Double.valueOf(percentageSD);
                    updated = true;
                }
                errorReport.append(
                    "percentage SD beyond bound, actual=" + percentageSD + ", upperbound=" + maxPercentageSD);
                errorReport.append("\n");
            }
            if (baselineMode && updated) {
                DiffRepository.writeDoubleArray(configFile, upLimitName + "_" + name(), upLimit);
            }
            return errorReport.toString();
        }

        public static String judgeAndReport(String upLimitName) throws IOException {
            StringBuilder error = new StringBuilder();
            StringBuilder sb = new StringBuilder("\n");
            for (QUANTIFIER_TYPE qt : QUANTIFIER_TYPE.values()) {
                String eMsg = qt.judge(upLimitName);
                if (!eMsg.isEmpty()) {
                    error.append(qt.name() + " error:" + eMsg);
                }
                AccuracyReport ar = qt.accuracyReportMap.get(upLimitName);
                if (ar == null) {
                    continue;
                }
                sb.append(qt.name()).append(":").append(ar.report()).append("\n");
            }
            // baseline skip error report
            if (!AccuracyQuantifier.baselineMode && error.length() > 0) {
                sb.append(error);
                Assert.fail(sb.toString());
            }
            return sb.toString();
        }

        public static void clean(String upLimitName) {
            for (QUANTIFIER_TYPE qt : QUANTIFIER_TYPE.values()) {
                qt.accuracyReportMap.remove(upLimitName);
            }
        }
    }

    static class AccuracyReport {
        private AtomicLong sumOfDifferences = new AtomicLong();
        private AtomicLong feedCount = new AtomicLong();
        private AtomicDouble sumOfPercentage = new AtomicDouble();

        private List<Double> diffList = Lists.newLinkedList();
        private List<Double> percentageList = Lists.newLinkedList();

        public void addDifferences(long diff) {
            sumOfDifferences.addAndGet(Math.abs(diff));
            diffList.add(Double.valueOf(diff));
        }

        public void feed(long estimate, long real) {
            addDifferences(estimate - real);
            feedCount.incrementAndGet();
            if (estimate == real) {
                percentageList.add(0.00);
                return;
            }
            double percentage = Math.abs(estimate);
            if (real != 0) {
                percentage = (double) Math.abs(estimate - real) / real;
            }
            sumOfPercentage.addAndGet(percentage);
            percentageList.add(percentage);
        }

        public static double getStandardDeviation(double[] x) {
            int m = x.length;
            double sum = 0;
            for (int i = 0; i < m; i++) {
                sum += x[i];
            }
            double dAve = sum / m;
            double dVar = 0;
            for (int i = 0; i < m; i++) {
                dVar += (x[i] - dAve) * (x[i] - dAve);
            }
            return Math.sqrt(dVar / m);
        }

        public String report() {
            NumberFormat numberFormat = NumberFormat.getPercentInstance();
            numberFormat.setMinimumFractionDigits(4);

            if (feedCount.get() == 0) {
                feedCount.incrementAndGet();
            }
            return "COUNT=" + feedCount
                + ",SUM OF DIFF=" + sumOfDifferences
                + ",DIFF σ=" + diffSD()
                + ",SUM OF PERCENTAGE=" + numberFormat.format(sumOfPercentage)
                + ",PERCENTAGE σ=" + percentageSD();
        }

        public long feedCount() {
            return feedCount.get();
        }

        public long sumOfDifferences() {
            return sumOfDifferences.get();
        }

        public double diffSD() {
            return getStandardDeviation(Doubles.toArray(diffList));
        }

        public double sumOfPercentage() {
            return sumOfPercentage.get();
        }

        public double percentageSD() {
            return getStandardDeviation(Doubles.toArray(percentageList));
        }

        public void clean() {
            sumOfDifferences.set(0);
            feedCount.set(0);
            sumOfPercentage.set(0);
        }

    }

    public void feed(String upLimitName, QUANTIFIER_TYPE qt, long estimate, long real) {
        qt.feed(upLimitName, estimate, real);
    }
}

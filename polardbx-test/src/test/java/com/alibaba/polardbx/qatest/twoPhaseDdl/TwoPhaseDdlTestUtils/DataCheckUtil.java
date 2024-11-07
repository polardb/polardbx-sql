package com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils;

import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlCheckApplicabilityTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DataCheckUtil {
    // Assume that there are two column, a int, b int, and a is primary key.
    final static Log log = LogFactory.getLog(TwoPhaseDdlCheckApplicabilityTest.class);
    final static Long FACTOR1 = 1000_000_007L;

    final static Long FACTOR2 = 192_608_17L;

    public static List<Integer> checkData(Connection tddlConnection, String schemaName, String mytable)
        throws SQLException {
        return checkData(tddlConnection, schemaName, mytable, "");
    }

    public static List<Integer> checkData(Connection tddlConnection, String schemaName, String mytable,
                                          String indexName)
        throws SQLException {
        log.info("start to check data");
        String calCheckSumSqlStmt =
            "select mod(sum(mod(a,  %d)), %d), mod(sum(mod(b, %d)), %d) from %s FORCE index(%s)";
        if (StringUtils.isEmpty(indexName)) {
            calCheckSumSqlStmt = "select mod(sum(mod(a,  %d)), %d), mod(sum(mod(b, %d)), %d) from %s";
        }
        String calCheckSumSql1 =
            String.format(calCheckSumSqlStmt, FACTOR1, FACTOR1, FACTOR1, FACTOR1, mytable, indexName);
        String calCheckSumSql2 =
            String.format(calCheckSumSqlStmt, FACTOR2, FACTOR2, FACTOR2, FACTOR2, mytable, indexName);
        List<Integer> checkSum1 =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(calCheckSumSql1, tddlConnection)).get(0).stream()
                .map(o -> Integer.valueOf(o.toString()))
                .collect(Collectors.toList());
        List<Integer> checkSum2 =
            JdbcUtil.getAllResult(JdbcUtil.executeQuery(calCheckSumSql2, tddlConnection)).get(0).stream()
                .map(o -> Integer.valueOf(o.toString()))
                .collect(Collectors.toList());
        log.info("finish check data");
        checkSum1.addAll(checkSum2);
        return checkSum1;
    }

    public static List<Integer> calCheckSumForSet(Map<Integer, Integer> keyValueMap) {
        Long sumOfKey = 0L;
        Long sumOfValue = 0L;
        List<Integer> results = new ArrayList<>();
        for (Integer key : keyValueMap.keySet()) {
            sumOfKey += key;
            sumOfValue += keyValueMap.get(key);
        }
        for (Long factor : Arrays.asList(FACTOR1, FACTOR2)) {
            results.add((int) (sumOfKey % factor));
            results.add((int) (sumOfValue % factor));
        }
        return results;
    }

    public static List<Integer> AddCheckSum(List<Integer> checkSum1, List<Integer> checkSum2) {
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            long checkSum = (checkSum1.get(i) + (long) (checkSum2.get(i))) % FACTOR1;
            results.add((int) checkSum);
        }
        for (int i = 2; i < checkSum1.size(); i++) {
            long checkSum = (checkSum1.get(i) + (long) (checkSum2.get(i))) % FACTOR2;
            results.add((int) checkSum);
        }
        return results;
    }

    public static List<Integer> MinusCheckSum(List<Integer> checkSum1, List<Integer> checkSum2) {
        List<Integer> results = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            long checkSum = ((long) checkSum1.get(i) + FACTOR1 - (long) (checkSum2.get(i))) % FACTOR1;
            results.add((int) checkSum);
        }
        for (int i = 2; i < checkSum1.size(); i++) {
            long checkSum = ((long) checkSum1.get(i) + FACTOR2 - (long) (checkSum2.get(i))) % FACTOR2;
            results.add((int) checkSum);
        }
        return results;
    }
}

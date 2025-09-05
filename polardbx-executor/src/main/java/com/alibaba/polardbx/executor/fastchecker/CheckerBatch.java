package com.alibaba.polardbx.executor.fastchecker;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Sets;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Created by taokun.
 *
 * @author taokun
 */
public class CheckerBatch {
    public String getPhyDb() {
        return phyDb;
    }

    public String getPhyTb() {
        return phyTb;
    }

    public List<Map<Integer, ParameterContext>> getBatchBound() {
        return batchBound;
    }

    public Map<String, Set<String>> getPhyTables(){
        Map<String, Set<String>> phyTables = new HashMap<>();
        Set<String> tables = Sets.newHashSet(phyTb);
        phyTables.put(phyDb, tables);
        return phyTables;
    }

    String phyDb;
    String phyTb;
    List<Map<Integer, ParameterContext>> batchBound;
    Boolean withUpperBound;
    Boolean withLowerBound;
    Integer batchIndex;
    Boolean isSourceTable;

    public CheckerBatch(String phyDb, String phyTb, int batchIndex, Boolean isSourceTable,
                        List<Map<Integer, ParameterContext>> batchBound,
                        Boolean withLowerBound, Boolean withUpperBound) {
        this.phyDb = phyDb;
        this.phyTb = phyTb;
        this.batchIndex = batchIndex;
        this.isSourceTable = isSourceTable;
        this.batchBound = batchBound;
        this.withUpperBound = withUpperBound;
        this.withLowerBound = withLowerBound;
    }

    public static CheckerBatch buildSourceBatchFromTargetBatch(CheckerBatch targetBatch,
                                                               Map<Pair<String, String>, List<Pair<String, String>>> topologyMap) {
        Pair<String, String> sourceTopology = topologyMap.get(Pair.of(targetBatch.phyDb, targetBatch.phyTb)).get(0);
        return new CheckerBatch(sourceTopology.getKey(), sourceTopology.getValue(), targetBatch.batchIndex, true,
            targetBatch.batchBound, targetBatch.withLowerBound, targetBatch.withUpperBound);
    }

    public Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> getBound() {
        Map<Integer, ParameterContext> firstBoundValue = null;
        Map<Integer, ParameterContext> secondBoundValue = null;
        if(batchBound != null) {
            if (withUpperBound && withLowerBound) {
                firstBoundValue = batchBound.get(0);
                secondBoundValue = batchBound.get(1);
            } else if (withLowerBound) {
                firstBoundValue = batchBound.get(0);
            } else if (withUpperBound) {
                secondBoundValue = batchBound.get(0);
            }
        }
        return Pair.of(firstBoundValue, secondBoundValue);

    }

    public String getBatchBoundDesc() {
        String firstBoundValue = "null";
        String lastBoundValue = "null";
        if (batchBound != null) {
            if (withUpperBound && withLowerBound) {
                firstBoundValue = GsiUtils.rowToString(batchBound.get(0));
                lastBoundValue = GsiUtils.rowToString(batchBound.get(1));
            } else if (withLowerBound) {
                firstBoundValue = GsiUtils.rowToString(batchBound.get(0));
            } else if (withUpperBound) {
                lastBoundValue = GsiUtils.rowToString(batchBound.get(0));
            }
        }
        String batchBoundDesc = String.format("%s.%s.{(%s), (%s)}", phyDb, phyTb, firstBoundValue, lastBoundValue);
        return batchBoundDesc;
    }

    // 重写equals方法
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CheckerBatch checkerBatch = (CheckerBatch) o;
        return phyDb.equalsIgnoreCase(((CheckerBatch) o).phyDb) && phyTb.equalsIgnoreCase(checkerBatch.phyTb)
            && batchIndex.equals(checkerBatch.batchIndex)
            && Objects.equals(batchBound, checkerBatch.batchBound)
            && withLowerBound.equals(checkerBatch.withLowerBound)
            && withUpperBound.equals(checkerBatch.withUpperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phyDb, phyTb, withLowerBound, withUpperBound, batchIndex, isSourceTable, batchBound);
    }

    public static Pair<Pair<String, String>, String> buildBatchReport(CheckerBatch sourceBatch, CheckerBatch targetBatch,
                                                                     Pair<Long, Boolean> sourceBatchResult,
                                                                     Pair<Long, Boolean> targetBatchResult) {
        String sourceValue = "";
        if (sourceBatchResult == null) {
            sourceValue = "UNKNOWN";
        } else {
            sourceValue = String.format("%s", sourceBatchResult.getKey());
        }
        String errorMsg =
            String.format("fastchecker found unconsistency, source batch hash: [%s] : %s, target batch hash [%s] : %s",
                sourceBatch.getBatchBoundDesc(), sourceValue, targetBatch.getBatchBoundDesc(),
                targetBatchResult.getKey()
            );
        SQLRecorderLogger.ddlLogger.error(errorMsg);
        Pair<Pair<String, String>, String> reportPair = Pair.of(Pair.of(sourceBatch.phyDb, sourceBatch.phyTb), errorMsg);
        return reportPair;
    }
}

package com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils;

import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlCheckApplicabilityTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class ChangeRowsState {
    final static Log log = LogFactory.getLog(TwoPhaseDdlCheckApplicabilityTest.class);

    public Map<Integer, Integer> rows;

    public List<Integer> primaryKeys;

    public int startPoint;

    public int endPoint;

    public int nextVal;

    public ChangeRowsState(Map<Integer, Integer> rows, int nextVal) {
        this.rows = rows;
        this.primaryKeys = new ArrayList<>(rows.keySet());
        this.startPoint = 0;
        this.endPoint = this.primaryKeys.size();
        this.nextVal = nextVal;
    }

    public int expandUnit = 8196;
    public List<Integer> expandUtilList = new ArrayList<>(Collections.nCopies(expandUnit, 0));

    public Random rand = new Random();

    public void updateByInsert(int key, int value) {
        if (this.primaryKeys.size() <= endPoint) {
            this.primaryKeys.addAll(expandUtilList);
        }
        this.primaryKeys.set(endPoint, key);
        this.nextVal++;
        this.endPoint++;
        this.rows.put(key, value);
    }

    public void updateByDelete(int key) {
        this.startPoint++;
    }

    public void updateByUpdate(int key, int value) {
        this.rows.put(key, value);
    }

    public List<Integer> fetchKeysForUpdate(int count) {
        List<Integer> updateKeys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int randomNum = rand.nextInt(endPoint - startPoint) + startPoint;
            updateKeys.add(this.primaryKeys.get(randomNum));
        }
        return updateKeys;
    }

    public List<Integer> fetchKeysForDelete(int count) {
        List<Integer> deleteKeys = this.primaryKeys.subList(startPoint, startPoint + count);
        return deleteKeys;
    }

    public int nextRand() {
        return rand.nextInt(1000_0000);
    }

    public void autoGc(int threadId) {
        if (this.primaryKeys.size() > (endPoint - startPoint) * 4) {
            log.info(String.format("threadId %d start to auto gc for changeRowsState with size %d", threadId,
                this.primaryKeys.size()));
            gc();
            log.info(String.format("threadId %d finish auto gc for changeRowsState with size %d", threadId,
                this.primaryKeys.size()));
        }
    }

    public void gc() {
        this.primaryKeys = primaryKeys.subList(startPoint, endPoint);
        Map<Integer, Integer> newRows = new HashMap<>();
        for (Integer primaryKey : this.primaryKeys) {
            newRows.put(primaryKey, rows.get(primaryKey));
        }
        rows = newRows;
        endPoint = endPoint - startPoint;
        startPoint = 0;
    }

    public void rollbackUpdate(int insertCount, int deleteCount, Map<Integer, Integer> updateSet) {
        endPoint -= insertCount;
        startPoint -= deleteCount;
        for (Integer primaryKey : updateSet.keySet()) {
            rows.put(primaryKey, updateSet.get(primaryKey));
        }
    }

    public List<Integer> updateCheckSum(Map<Integer, Integer> beforeRows, List<Integer> checkSum) {
        if (this.primaryKeys.size() > (endPoint - startPoint)) {
            gc();
        }
        Set<Integer> afterPrimaryKeys = new HashSet<>(primaryKeys);
        Map<Integer, Integer> deleteKeys = new HashMap<>();
        Map<Integer, Integer> insertKeys = new HashMap<>();
        for (Integer key : beforeRows.keySet()) {
            if (afterPrimaryKeys.contains(key)) {
                int beforeValue = beforeRows.get(key);
                int afterValue = rows.get(key);
                if (beforeValue != afterValue) {
                    deleteKeys.put(key, beforeValue);
                    insertKeys.put(key, afterValue);
                }
                afterPrimaryKeys.remove(key);
            } else {
                int beforeValue = beforeRows.get(key);
                deleteKeys.put(key, beforeValue);
            }
        }
        for (Integer key : afterPrimaryKeys) {
            insertKeys.put(key, rows.get(key));
        }

        List<Integer> insertedCheckSum = DataCheckUtil.calCheckSumForSet(insertKeys);
        List<Integer> deletedCheckSum = DataCheckUtil.calCheckSumForSet(deleteKeys);
        return DataCheckUtil.MinusCheckSum(DataCheckUtil.AddCheckSum(checkSum, insertedCheckSum), deletedCheckSum);
    }

}

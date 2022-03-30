/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.planmanager.parametric;

import com.alibaba.polardbx.optimizer.planmanager.feedback.PhyFeedBack;

import java.util.List;
import java.util.Map;

public class Point {
    private final String parameterSql;
    private final Map<String, Double> selectivityMap;
    private final List<Object> params;
    private volatile int planId = -1;
    private double rowcountExpected = -1D;
    private double maxRowcountExpected = -1D;
    private double minRowcountExpected = -1D;
    private PhyFeedBack phyFeedBack = null;
    private long chooseTime = 0L;
    private final double[] selectivityArray;
    private double inflationNarrow = 1;

    private int recentlyChooseTime = 0;
    private double lastRecentlyChooseRate = -1;
    private int unuseTermTime;
    private boolean isSteady = false;

    public Point(String parameterSql, Map<String, Double> selectivityMap, List<Object> params) {
        this.parameterSql = parameterSql;
        this.selectivityMap = selectivityMap;
        this.params = params;
        int count = 0;
        selectivityArray = new double[selectivityMap.size()];
        for (double selectivity : getSelectivityMap().values()) {
            selectivityArray[count++] = selectivity;
        }
    }

    public Point(String parameterSql, Map<String, Double> selectivityMap, List<Object> params, int planId,
                 double rowcountExpected, long chooseTime, double maxRowcountExpected, double minRowcountExpected,
                 PhyFeedBack phyFeedBack) {
        this(parameterSql, selectivityMap, params);
        this.planId = planId;
        this.rowcountExpected = rowcountExpected;
        this.chooseTime = chooseTime;
        this.maxRowcountExpected = maxRowcountExpected;
        this.minRowcountExpected = minRowcountExpected;
        this.phyFeedBack = phyFeedBack;
    }

    public String getParameterSql() {
        return parameterSql;
    }

    public Map<String, Double> getSelectivityMap() {
        return selectivityMap;
    }

    public List<Object> getParams() {
        return params;
    }

    public long getPlanId() {
        return planId;
    }

    public void setPlanId(int planId) {
        this.planId = planId;
    }

    public double getRowcountExpected() {
        return rowcountExpected;
    }

    public void setRowcountExpected(double rowcountExpected) {
        this.rowcountExpected = rowcountExpected;
    }

    public long getChooseTime() {
        return chooseTime;
    }

    public void increaseChooseTime(long chooseTime) {
        this.chooseTime += chooseTime;
        this.recentlyChooseTime += chooseTime;
    }

    public double getMaxRowcountExpected() {
        return maxRowcountExpected;
    }

    public void setMaxRowcountExpected(double maxRowcountExpected) {
        this.maxRowcountExpected = maxRowcountExpected;
    }

    public double getMinRowcountExpected() {
        return minRowcountExpected;
    }

    public void setMinRowcountExpected(double minRowcountExpected) {
        this.minRowcountExpected = minRowcountExpected;
    }

    public void feedBackRowcount(double rowcountFeedBack) {
        if (getRowcountExpected() == -1D) {
            setMinRowcountExpected(rowcountFeedBack);
            setMaxRowcountExpected(rowcountFeedBack);
            setRowcountExpected(rowcountFeedBack);
        } else {
            if (getMaxRowcountExpected() < rowcountFeedBack) {
                setMaxRowcountExpected(rowcountFeedBack);
            }

            if (getMinRowcountExpected() > rowcountFeedBack || getMinRowcountExpected() == -1D) {
                setMinRowcountExpected(rowcountFeedBack);
            }
        }
    }

    public void phyFeedBack(PhyFeedBack phyFeedBack) {
        this.phyFeedBack = phyFeedBack;
    }

    public void updateLastRecentlyChooseTime(int chooseTimeSum) {
        this.lastRecentlyChooseRate = Double.valueOf(recentlyChooseTime) / chooseTimeSum;
        this.recentlyChooseTime = 0;
        if (lastRecentlyChooseRate < 0.01) {
            unuseTermTime++;
        } else {
            unuseTermTime = 0;
        }
    }

    public double[] getSelectivityArray() {
        return selectivityArray;
    }

    public double getInflationNarrow() {
        return inflationNarrow;
    }

    public void setInflationNarrow(double inflationNarrow) {
        this.inflationNarrow = inflationNarrow;
    }

    public int getRecentlyChooseTime() {
        return recentlyChooseTime;
    }

    public void setRecentlyChooseTime(int recentlyChooseTime) {
        this.recentlyChooseTime = recentlyChooseTime;
    }

    public double getLastRecentlyChooseRate() {
        return lastRecentlyChooseRate;
    }

    public void setLastRecentlyChooseRate(double lastRecentlyChooseRate) {
        this.lastRecentlyChooseRate = lastRecentlyChooseRate;
    }

    public int getUnuseTermTime() {
        return unuseTermTime;
    }

    public void setUnuseTermTime(int unuseTermTime) {
        this.unuseTermTime = unuseTermTime;
    }

    public PhyFeedBack getPhyFeedBack() {
        return phyFeedBack;
    }

    public boolean isSteady() {
        return isSteady;
    }

    public void setSteady(boolean steady) {
        isSteady = steady;
    }
}
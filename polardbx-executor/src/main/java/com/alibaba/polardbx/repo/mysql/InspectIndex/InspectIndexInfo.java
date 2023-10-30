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

package com.alibaba.polardbx.repo.mysql.InspectIndex;

import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class InspectIndexInfo {

    public enum BadIndexKind {
        LFU, //Least Frequently Used
        LRU, // Least Recently Used
        LOW_DISCRIMINATION,

        /**
         * 低效的index column，比如key分区的gsi，用秒级字段做索引列
         */
        INEFFECTIVE_GSI,
        HOTSPOT,
        DUPLICATE_GSI,

        DUP_GSI_INDEX_COLUMN,
        DUPLICATE_LSI,
        PRIMERY_INDEX_AS_PREFIX_LSI
    }

    public boolean isPartitionTable;

    public boolean unique;

    public boolean clustered;

    public String tableName;
    public String indexName;
    public List<String> indexColumns;
    public List<String> coveringColumns;
    public Set<String> primaryTableAllColumns;

    public List<String> dbPartitionColumns;
    public String dbPartitionPolicy;
    public String dbPartitionCount;
    public List<String> tbPartitionColumns;
    public String tbPartitionPolicy;
    public String tbPartitionCount;

    public String partitionSql;

    public Long useCount;
    public Date accessTime;
    public Long rowCardinality;
    public Long rowCount;
    public Double rowDiscrimination;

    public Map<BadIndexKind, String> problem;

    public List<String> adviceIndexColumns;
    public List<String> adviceCoveringColumns;

    public List<String> invisibleIndexAdvice;
    public List<String> advice;

    public boolean needGenLsi;

    public InspectIndexInfo() {
        this.indexColumns = new ArrayList<>();
        this.coveringColumns = new ArrayList<>();
        this.primaryTableAllColumns = new TreeSet<>(String::compareToIgnoreCase);
        this.dbPartitionColumns = new ArrayList<>();
        this.tbPartitionColumns = new ArrayList<>();
        this.problem = new TreeMap<>();
        this.advice = new ArrayList<>();
        this.adviceIndexColumns = new ArrayList<>();
        this.adviceCoveringColumns = new ArrayList<>();
        this.invisibleIndexAdvice = new ArrayList<>();
        this.needGenLsi = true;
    }

    public void generateAdvice() {
        if (problem.containsKey(BadIndexKind.LFU)
            || problem.containsKey(BadIndexKind.LRU)) {
            this.invisibleIndexAdvice.add(generateInvisibleGsiSql());
            this.advice.add(generateDropSql());
        } else if (problem.containsKey(BadIndexKind.DUPLICATE_GSI)) {
            if (!adviceCoveringColumns.isEmpty()) {
                this.invisibleIndexAdvice.add(generateInvisibleGsiSql());
                this.advice.add(generateDropSql());
                this.advice.add(generateAddGsiSql());
            } else {
                this.invisibleIndexAdvice.add(generateInvisibleGsiSql());
                this.advice.add(generateDropSql());
                if (problem.containsKey(BadIndexKind.INEFFECTIVE_GSI) && this.needGenLsi) {
                    this.advice.add(generateAddLsiSql());
                }
            }
        } else if (problem.containsKey(BadIndexKind.DUP_GSI_INDEX_COLUMN)) {
            if (!(adviceCoveringColumns.isEmpty() && adviceIndexColumns.isEmpty())) {
                this.invisibleIndexAdvice.add(generateInvisibleGsiSql());
                this.advice.add(generateDropSql());
                this.advice.add(generateAddGsiSql());
            } else {
                this.invisibleIndexAdvice.add(generateInvisibleGsiSql());
                this.advice.add(generateDropSql());
                if (this.needGenLsi) {
                    this.advice.add(generateAddLsiSql());
                }
            }
        } else if (problem.containsKey(BadIndexKind.HOTSPOT) || problem.containsKey(BadIndexKind.LOW_DISCRIMINATION)) {
            if (problem.containsKey(BadIndexKind.HOTSPOT)) {
                //this.advice.add("please adjust partition columns;");
                this.invisibleIndexAdvice.add(generateInvisibleGsiSql());
                this.advice.add(generateDropSql());
            }
            if (problem.containsKey(BadIndexKind.LOW_DISCRIMINATION)) {
                //this.advice.add("please adjust index columns;");
                this.invisibleIndexAdvice.add(generateInvisibleGsiSql());
                this.advice.add(generateDropSql());
            }
        } else if (problem.containsKey(BadIndexKind.DUPLICATE_LSI)) {
            this.advice.add(generateDropSql());
        } else if (problem.containsKey(BadIndexKind.PRIMERY_INDEX_AS_PREFIX_LSI)) {
            this.advice.add(generateDropSql());
            this.advice.add(generateAddLsiSql());
        }
    }

    protected String generateDropSql() {
        return "alter table `" + tableName + "` drop index `" + getLogicalGsiName() + "`;";
    }

    protected String generateInvisibleGsiSql() {
        return "alter table `" + tableName + "` alter index `" + getLogicalGsiName() + "` invisible;";
    }

    protected String generateAddGsiSql() {
        final String sql = "alter table `" + tableName + "` add global index `" + getLogicalGsiName() + "` (%s)";
        final String covering = " covering(%s)";
        final String partition = " " + this.partitionSql;
        final String cgsiSql = "alter table `" + tableName + "` add clustered index `" + getLogicalGsiName() + "` (%s)";
        final String ugsiSql =
            "alter table `" + tableName + "` add unique global index `" + getLogicalGsiName() + "` (%s)";
        final String ucgsiSql =
            "alter table `" + tableName + "` add unique clustered index `" + getLogicalGsiName() + "` (%s)";

        Set<String> allColumnsOnGsi = new TreeSet<>(String::compareToIgnoreCase);
        allColumnsOnGsi.addAll(this.adviceCoveringColumns);
        if (!adviceIndexColumns.isEmpty()) {
            allColumnsOnGsi.addAll(this.adviceIndexColumns);
        } else {
            allColumnsOnGsi.addAll(this.indexColumns);
        }

        int allColLen = this.primaryTableAllColumns.size();
        int coveringColLen = allColumnsOnGsi.size();
        boolean needClustered = allColumnsOnGsi.equals(this.primaryTableAllColumns)
            || (allColLen > 8 && (double) coveringColLen / allColLen >= 0.8);

        String result = "";
        List<String> indexColumnsWithQuote;

        if (!adviceIndexColumns.isEmpty()) {
            indexColumnsWithQuote = adviceIndexColumns
                .stream().map(col -> "`" + col + "`").collect(Collectors.toList());
        } else {
            indexColumnsWithQuote = indexColumns
                .stream().map(col -> "`" + col + "`").collect(Collectors.toList());
        }

        if (this.unique) {
            if (needClustered) {
                result += String.format(ucgsiSql, String.join(",", indexColumnsWithQuote));
            } else {
                result += String.format(ugsiSql, String.join(",", indexColumnsWithQuote));
            }
        } else {
            if (needClustered) {
                result += String.format(cgsiSql, String.join(",", indexColumnsWithQuote));
            } else {
                result += String.format(sql, String.join(",", indexColumnsWithQuote));
            }
        }

        List<String> coveringColumnsWithQuote;
        if (!this.adviceCoveringColumns.isEmpty() && !needClustered) {
            coveringColumnsWithQuote = adviceCoveringColumns
                .stream().map(col -> "`" + col + "`").collect(Collectors.toList());
            result += String.format(covering, String.join(",", coveringColumnsWithQuote));
        }

        if (!StringUtil.isNullOrEmpty(partition)) {
            result += partition;
        }
        return result + ";";
    }

    protected String generateAddLsiSql() {
        final String sql = "alter table `" + tableName + "` add local index `" + getLogicalGsiName() + "` (%s)";
        final String uk = "alter table `" + tableName + "` add unique local index `" + getLogicalGsiName() + "` (%s)";
        String result = "";
        List<String> columnsWithQuote;
        if (!this.adviceIndexColumns.isEmpty()) {
            columnsWithQuote = adviceIndexColumns.stream().map(col -> "`" + col + "`").collect(Collectors.toList());
        } else {
            columnsWithQuote = indexColumns.stream().map(col -> "`" + col + "`").collect(Collectors.toList());
        }

        if (unique) {
            result += String.format(uk, String.join(",", columnsWithQuote));
        } else {
            result += String.format(sql, String.join(",", columnsWithQuote));
        }
        return result + ";";
    }

    public String getLogicalGsiName() {
        final String gsiName = this.indexName;
        if (!isPartitionTable || gsiName.length() <= 6) {
            return gsiName;
        }
        String suffix = gsiName.substring(gsiName.length() - 6);
        if (suffix.startsWith("_$")) {
            return gsiName.substring(0, gsiName.length() - 6);
        } else {
            return gsiName;
        }
    }
}

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

package com.alibaba.polardbx.server.ugly.hint;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.parse.util.Pair;

/**
 * @author QIU Shuo
 */
public final class CobarHint {

    // index start from 1
    // /*!drds: $dataNodeId=0.0*/ SQL在第一个数据节点的第一个备份上执行
    // /*!drds: $dataNodeId=0.0, $table='offer'*/ offer表SQL在第一个数据节点的第一个备份上执行
    // /*!drds: $dataNodeId=[0,1,5.2], $table='offer'*/
    // offer表SQL在第一个数据节点的第一个备份，第二个数据节点的第一个备份，第六个数据节点的第三个备份执行
    // /*!drds: $partitionOperand=('member_id'='m1'), $table='offer'*/
    // 计算offer表的m1拆分键值的路由，然后在目标库的第一个备份上执行
    // /*!drds: $partitionOperand=('member_id'=['m1','m2']), $table='offer',
    // $replica=2*/ 计算offer表的m1,m2拆分键值的路由，然后在每一个目标库的第三个备份上执行
    // /*!drds: $partitionOperand=(['offer_id','group_id']=[123,'3c']),
    // $table='offer'*/ offer_id的值是123，group_id是3c
    // /*!drds:
    // $partitionOperand=(['offer_id','group_id']=[[123,'3c'],[234,'food']]),
    // $table='offer'*/ offer_id的值是123,234;group_id的值是3c,food

    public static final String                   COBAR_HINT_UGLY_PREFIX = "/!drds:";
    public static final String                   COBAR_HINT_PREFIX      = "/*!drds:";
    public static final int                      DEFAULT_REPLICA_INDEX  = -1;
    private static final Map<String, HintParser> HINT_PARSERS           = new HashMap<String, HintParser>();
    {
        HINT_PARSERS.put("table", new SimpleHintParser());
        HINT_PARSERS.put("replica", new SimpleHintParser());
        HINT_PARSERS.put("dataNodeId", new DataNodeHintParser());
        HINT_PARSERS.put("partitionOperand", new PartitionOperandHintParser());
    }

    private int                                  replica                = DEFAULT_REPLICA_INDEX;
    private String                               table;
    private List<Pair<Integer, Integer>>         dataNodes;
    private Pair<String[], Object[][]>           partitionOperand;

    /**
     * @param offset index of first char of {@link #COBAR_HINT_PREFIX}
     */
    public static CobarHint parserCobarHint(String sql, int offset) throws SQLSyntaxErrorException {
        CobarHint hint = new CobarHint();
        hint.currentIndex = offset;
        hint.parse(sql);
        return hint;
    }

    /**
     * @return String[] in upper-case
     */
    public Pair<String[], Object[][]> getPartitionOperand() {
        return partitionOperand;
    }

    public void setPartitionOperand(Pair<String[], Object[][]> partitionOperand) {
        String[] columns = partitionOperand.getKey();
        if (columns == null) {
            this.partitionOperand = partitionOperand;
        } else {
            String[] colUp = new String[columns.length];
            for (int i = 0; i < columns.length; ++i) {
                colUp[i] = columns[i].toUpperCase();
            }
            this.partitionOperand = new Pair<String[], Object[][]>(colUp, partitionOperand.getValue());
        }
    }

    public List<Pair<Integer, Integer>> getDataNodes() {
        return dataNodes;
    }

    public void addDataNode(Integer dataNodeIndex, Integer replica) {
        if (dataNodeIndex == null) {
            throw new IllegalArgumentException("data node index is null");
        }
        if (replica == DEFAULT_REPLICA_INDEX || replica < 0) {
            replica = null;
        }
        if (dataNodes == null) {
            dataNodes = new ArrayList<Pair<Integer, Integer>>();
        }
        dataNodes.add(new Pair<Integer, Integer>(dataNodeIndex, replica));
    }

    /**
     * @return upper case
     */
    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table.toUpperCase();
    }

    public int getReplica() {
        return replica;
    }

    public void setReplica(int replica) {
        this.replica = replica;
    }

    private void parse(String sql) throws SQLSyntaxErrorException {
        cobarHint: for (;;) {
            skip: for (;;) {
                switch (sql.charAt(currentIndex)) {
                    case '$':
                        break skip;
                    case '*':
                        currentIndex += 2;
                        break cobarHint;
                    default:
                        ++currentIndex;
                }
            }
            int hintNameEnd = sql.indexOf('=', currentIndex);
            String hintName = sql.substring(currentIndex + 1, hintNameEnd).trim();
            HintParser hintParser = HINT_PARSERS.get(hintName);
            if (hintParser != null) {
                currentIndex = 1 + sql.indexOf('=', hintNameEnd);
                hintParser.process(this, hintName, sql);
            } else {
                throw new SQLSyntaxErrorException("unrecognized hint name: ${" + hintName + "}");
            }
        }
        outputSql = sql.substring(currentIndex);
    }

    private String outputSql;
    private int    currentIndex;

    public int getCurrentIndex() {
        return currentIndex;
    }

    public CobarHint increaseCurrentIndex() {
        ++currentIndex;
        return this;
    }

    public void setCurrentIndex(int ci) {
        currentIndex = ci;
    }

    public String getOutputSql() {
        return outputSql;
    }
}

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

package com.alibaba.polardbx.optimizer.parse.bean;

import java.util.List;

public class DBPartitionOptions {

    /**
     * <pre>
     * 90%情况都是按照HASH(用户)切，需求是如果数据量太大，就继续按照RANGE(时间)切，系统往小了做，
     * 尽量保证DRDS和TDDL规则的不变，将来方便引导用户升级，为此我们可以对mysql的语法进行扩充.
     * 不能将同一个字段的选择对应到多个Rule bean上，但不同得得字段可以对应到不同的Rule bean规则上，
     * 比如如果where条件有id可以走id的规则，如果有userid可以走userid的规则，也可以如省略dbNamePattern
     * 直接用dbRuleArray返回的groupName如dbRuleArray=id>1 & id <3 return group1
     *
     * 现在可以只实现1)HASH 2)RANGE 3)FREE 4)multi-DBPARTITIONS
     *
     *
     * PARTITION分库  SUBPARTITION分表
     * 1) 只分库，不分表
     *   DBPARTITION BY HASH(user_name)
     *   DBPARTITIONS 1024  #分库数
     *   ( DBPARTITION group_name_pattern  #只能对应一个rule bean，可以是固定group也可以是group占位
     *   )
     *
     * 2) 分库分表模式(16个分库,每个分库64个分表,生成的规则: #user_name,1,16*64#.hash_code().abs()% (16*64))
     *   DBPARTITION BY HASH(user_name)
     *   DBPARTITIONS 16
     *   TBPARTITION BY HASH(user_name)
     *   TBPARTITIONS 64
     *   ( DBPARTITION group_name_pattern  #只能对应一个rule bean，可以是固定group也可以是group占位
     *     ( TBPARTITION table_name_pattern #只能有一个,可以是固定table也可以是table占位
     *     )
     *   )
     *
     * 3) HASH(MONTH(tr_date)) 支持自定义分片预发，内置函数参考GroovyStaticMethod.java
     *
     * 4) 支持自由指定分库/分表规则
     *   DBPARTITION BY FREE(expression)
     *   TBPARTITION BY FREE(expression)
     *   ( DBPARTITION group_name_pattern
     *     ( TBPARTITION table_name_pattern
     *     )
     *   )
     *
     * 5) 支持GROUP分库,不分表
     *   DBPARTITION BY RANGE(timestamp)
     *   ( DBPARTITION group_name1 VALUES LESS THAN 1,
     *     DBPARTITION group_name2 VALUES LESS THAN 100,
     *     DBPARTITION group_name3 VALUES LESS THAN MAXVALUE
     *   )
     *
     * 6 支持GROUP分库,分表???都按照RANGE还是HASH+RANGE???
     *   DBPARTITION BY RANGE(timestamp)
     *   TBPARTITION BY HASH(user_name)
     *   TBPARTITIONS 64
     *   ( DBPARTITION group_name1 VALUES LESS THAN 1,
     *     DBPARTITION group_name2 VALUES LESS THAN 100,
     *     DBPARTITION group_name3 VALUES LESS THAN MAXVALUE
     * )
     * </pre>
     */

    // public List<DBPartitionDefinition> getDbpartitionDefinitionList() {
    // return dbpartitionDefinitionList;
    // }
    //
    // public void setDbpartitionDefinitionList(List<DBPartitionDefinition>
    // dbpartitionDefinitionList) {
    // this.dbpartitionDefinitionList = dbpartitionDefinitionList;

    private DBPartitionBy dbpartitionBy;
    private Integer dbpartitions;
    private TBPartitionBy tbpartitionBy;
    private Integer tbpartitions;

    private Integer startWith;
    private Integer endWith;

    private List<DBPartitionDefinition> dbpartitionDefinitionList;

    public DBPartitionBy getDbpartitionBy() {
        return dbpartitionBy;
    }

    public void setDbpartitionBy(DBPartitionBy dbpartitionBy) {
        this.dbpartitionBy = dbpartitionBy;
    }

    public Integer getDbpartitions() {
        return dbpartitions;
    }

    public void setDbpartitions(int dbpartitions) {
        this.dbpartitions = dbpartitions;
    }

    public TBPartitionBy getTbpartitionBy() {
        return tbpartitionBy;
    }

    public void setTbpartitionBy(TBPartitionBy tbpartitionBy) {
        this.tbpartitionBy = tbpartitionBy;
    }

    public Integer getTbpartitions() {
        return tbpartitions;
    }

    public void setTbpartitions(int tbpartitions) {
        this.tbpartitions = tbpartitions;
    }

    // }

    public Integer getStartWith() {
        return startWith;
    }

    public void setStartWith(Integer startWith) {
        this.startWith = startWith;
    }

    public Integer getEndWith() {
        return endWith;
    }

    public void setEndWith(Integer endWith) {
        this.endWith = endWith;
    }

    public List<DBPartitionDefinition> getDbpartitionDefinitionList() {
        return dbpartitionDefinitionList;
    }

    public void setDbpartitionDefinitionList(List<DBPartitionDefinition> dbpartitionDefinitionList) {
        this.dbpartitionDefinitionList = dbpartitionDefinitionList;
    }
}

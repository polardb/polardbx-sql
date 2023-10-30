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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.Pair;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author wumu
 */
@Data
public class RepartitionPrepareData extends DdlPreparedData {

    /**
     * gsi which need to be dropped
     */
    private List<String> droppedIndexes;

    /**
     * gsi which need to add columns and backfill
     * key: global index name
     * value: columns list
     */
    private Map<String, List<String>> backFilledIndexes;

    /**
     * gsi which need to drop columns
     * key: global index name
     * value: columns list
     */
    private Map<String, List<String>> dropColumnIndexes;

    /**
     * change gsi to local index when alter table single/broadcast
     * key: global index name
     * value: Pair<index columns name, gsi nonUnique>
     */
    private Map<String, Pair<List<String>, Boolean>> gsiInfo;

    private String primaryTableDefinition;

    /**
     * optimize repartition for key partition table
     */
    private List<String> changeShardColumnsOnly;

    private Pair<String, String> addLocalIndexSql;

    private Pair<String, String> dropLocalIndexSql;

    /**
     * change foreign key to physical or logical when repartition
     * Foreign key.
     */
    private List<ForeignKeyData> modifyForeignKeys = new ArrayList<>();

    private List<Pair<String, String>> addForeignKeySql = new ArrayList<>();

    private List<Pair<String, String>> dropForeignKeySql = new ArrayList<>();

    private Map<String, Set<String>> foreignKeyChildTable = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private Boolean modifyLocality;
}

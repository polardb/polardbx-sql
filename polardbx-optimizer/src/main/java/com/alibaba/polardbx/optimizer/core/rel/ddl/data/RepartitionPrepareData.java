package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.optimizer.parse.util.Pair;
import lombok.Data;

import java.util.List;
import java.util.Map;

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
     * change gsi to local index when alter table single/broadcast
     * key: global index name
     * value: local index columns name
     */
    private Map<String, List<String>> localIndexes;

    private String primaryTableDefinition;

    /**
     * optimize repartition for key partition table
     */
    private List<String> changeShardColumnsOnly;

    private Pair<String, String> addLocalIndexSql;

    private Pair<String, String> dropLocalIndexSql;

}

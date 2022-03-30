package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AlterTableGroupSplitPartitionByHotValuePreparedData extends AlterTableGroupBasePreparedData {

    int[] insertPos;

    Map<String, List<Long[]>> splitPointInfos;

    boolean skipSplit;
    String hotKeyPartitionName;

    public AlterTableGroupSplitPartitionByHotValuePreparedData() {
    }

    public int[] getInsertPos() {
        return insertPos;
    }

    public void setInsertPos(int[] insertPos) {
        this.insertPos = insertPos;
    }

    public boolean isSkipSplit() {
        return skipSplit;
    }

    public void setSkipSplit(boolean skipSplit) {
        this.skipSplit = skipSplit;
    }

    public String getHotKeyPartitionName() {
        return hotKeyPartitionName;
    }

    public void setHotKeyPartitionName(String hotKeyPartitionName) {
        this.hotKeyPartitionName = hotKeyPartitionName;
    }

    public boolean hotPartitionNameNeedChange() {
        if (StringUtils.isNotEmpty(hotKeyPartitionName)) {
            List<String> oldPartNames = getOldPartitionNames();
            List<String> newPartNames = getNewPartitionNames();
            if (GeneralUtil.isNotEmpty(oldPartNames) && GeneralUtil.isNotEmpty(newPartNames)
                && oldPartNames.size() == newPartNames.size()) {
                for (int i = 0; i < oldPartNames.size(); i++) {
                    if (!oldPartNames.get(i).equalsIgnoreCase(newPartNames.get(i))) {
                        return true;
                    }
                }
                return false;
            }
        }
        return false;
    }

    public List<Pair<String, String>> getChangeHotPartitionNames() {
        List<Pair<String, String>> changePartitionsPair = new ArrayList<>();
        List<String> oldPartNames = getOldPartitionNames();
        List<String> newPartNames = getNewPartitionNames();
        if (GeneralUtil.isNotEmpty(oldPartNames) && GeneralUtil.isNotEmpty(newPartNames)
            && oldPartNames.size() == newPartNames.size()) {
            for (int i = 0; i < oldPartNames.size(); i++) {
                Pair<String, String> pair = new Pair<>(oldPartNames.get(i), newPartNames.get(i));
                changePartitionsPair.add(pair);
                PartitionNameUtil.validatePartName(pair.getValue(), KeyWordsUtil.isKeyWord(pair.getValue()));
            }
        }
        return changePartitionsPair;
    }

    public Map<String, List<Long[]>> getSplitPointInfos() {
        return splitPointInfos;
    }

    public void setSplitPointInfos(Map<String, List<Long[]>> splitPointInfos) {
        this.splitPointInfos = splitPointInfos;
    }
}

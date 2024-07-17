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

package com.alibaba.polardbx.common.jdbc;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.google.common.collect.Lists;

import java.util.BitSet;
import java.util.List;

/**
 * @author fangwu
 */
public class PruneRawString extends RawString {
    protected PRUNE_MODE pruneMode;

    /**
     * for RANGE mode
     */
    private int startIndex;
    private int endIndex;

    /**
     * for MULTI_INDEX mode
     */
    private BitSet indexes;

    public PruneRawString(List list, PRUNE_MODE pruneMode, int startIndex, int endIndex, BitSet indexes) {
        super(list);
        this.pruneMode = pruneMode;

        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.indexes = indexes;

        paramCheck();
    }

    @Override
    public Object getObj(int index, int skIndex) {
        switch (pruneMode) {
        case RANGE:
            /**
             * param range check
             */
            if (index >= (this.endIndex - this.startIndex)) {
                GeneralUtil.nestedException(
                    "PruneRawString error array index out of bounds:" + index + "," + display());
            }
            index += this.startIndex;
            return super.getObj(index, skIndex);
        case MULTI_INDEX:
            /**
             * param range check
             */
            if (index >= indexes.cardinality()) {
                GeneralUtil.nestedException(
                    "PruneRawString error array index out of bounds:" + index + "," + display());
            }
            int i = indexes.nextSetBit(0);
            int currentIndex = 0;
            while (currentIndex < index) {
                currentIndex++;
                i = indexes.nextSetBit(i + 1);
            }
            return super.getObj(i, skIndex);
        default:
            throw GeneralUtil.nestedException("not support yet:" + index + "," + display());
        }
    }

    @Override
    public String display() {
        StringBuilder stringBuilder = new StringBuilder();
        if (size() == super.getObjList().size()) {
            stringBuilder.append("NonPruneRaw(" + buildRawString() + ")");
        } else {
            stringBuilder.append("PruneRaw(" + buildRawString() + ")");
        }
        if (stringBuilder.length() > 4096) {
            return stringBuilder.substring(0, 4096) + "...";
        }
        return stringBuilder.toString();
    }

    @Override
    public List getObjList() {
        switch (pruneMode) {
        case RANGE:
            return super.getObjList().subList(startIndex, endIndex);
        case MULTI_INDEX:
            List<Object> list = Lists.newArrayList();
            int i = indexes.nextSetBit(0);
            while (i != -1) {
                list.add(super.getObjList().get(i));
                i = indexes.nextSetBit(i + 1);
            }
            return list;
        default:
            throw GeneralUtil.nestedException("not support yet:" + "," + display());
        }
    }

    @Override
    public int size() {
        switch (pruneMode) {
        case RANGE:
            return endIndex - startIndex;
        case MULTI_INDEX:
            return indexes.cardinality();
        default:
            throw GeneralUtil.nestedException("not support yet:" + "," + display());
        }
    }

    @Override
    public String buildRawString(BitSet x) {
        if (x == null || x.size() == 0) {
            GeneralUtil.nestedException("empty param list");
        }
        List<Integer> integers = Lists.newArrayList();
        BitSet b = new BitSet();
        switch (pruneMode) {
        case RANGE:
            int i = x.nextSetBit(0);
            while (i != -1) {
                if (i >= (endIndex - startIndex)) {
                    throw GeneralUtil.nestedException("unmatch obj index in PruneRawString:" + i + "," + display());
                }
                b.set(i + startIndex);
                i = x.nextSetBit(i + 1);
            }
            return super.buildRawString(b);
        case MULTI_INDEX:
            i = x.nextSetBit(0);
            int currentIndex = 0;
            int currentPosition = 0;
            while (i != -1) {
                if (i >= indexes.cardinality()) {
                    throw GeneralUtil.nestedException("unmatch obj index in PruneRawString:" + i + "," + display());
                }
                while (currentIndex < i) {
                    currentPosition = indexes.nextSetBit(currentPosition + 1);
                    currentIndex++;
                }
                b.set(currentPosition);
                i = x.nextSetBit(i + 1);
            }
            return super.buildRawString(b);
        default:
            throw GeneralUtil.nestedException("not support yet:" + "," + display());
        }
    }

    @Override
    public String buildRawString() {
        switch (pruneMode) {
        case RANGE:
            BitSet b = new BitSet();
            for (int i = startIndex; i < endIndex; i++) {
                b.set(i);
            }
            return super.buildRawString(b);
        case MULTI_INDEX:
            return super.buildRawString(indexes);
        default:
            throw GeneralUtil.nestedException("not support yet:" + "," + display());
        }
    }

    private void paramCheck() {
        switch (pruneMode) {
        case RANGE:
            if (!(startIndex >= 0 && startIndex < super.getObjList().size()
                && endIndex >= 0 && endIndex <= super.getObjList().size()
                && endIndex > startIndex)) {
                GeneralUtil.nestedException(
                    "RawString init ERROR RANGE mode with invalid start/end index:" + startIndex + "," + endIndex + ","
                        + getObjList().size());
            }
            return;
        case MULTI_INDEX:
            if (indexes == null || indexes.isEmpty()) {
                GeneralUtil.nestedException("RawString init ERROR MUITI_INDEX mode with invalid indexes:" + indexes);
            }
            return;
        default:
            GeneralUtil.nestedException("RawString init ERROR mode invalid :" + pruneMode);
        }
    }

    public void merge(PruneRawString pruneRawString) {
        if (size() == super.getObjList().size()) {
            return;
        }
        if (pruneMode == PRUNE_MODE.RANGE) {
            transformModeToMultiIndex();
        }
        if (pruneRawString.pruneMode == PRUNE_MODE.RANGE) {
            pruneRawString.transformModeToMultiIndex();
        }
        indexes.or(pruneRawString.indexes);
        if (indexes.cardinality() == super.getObjList().size()) {
            pruneMode = PRUNE_MODE.RANGE;
            startIndex = 0;
            endIndex = super.getObjList().size();
        }
    }

    private void transformModeToMultiIndex() {
        indexes = new BitSet();

        switch (pruneMode) {
        case RANGE:
            for (int i = startIndex; i < endIndex; i++) {
                indexes.set(i);
            }
        case MULTI_INDEX:
        default:
            // donothing
        }
        pruneMode = PRUNE_MODE.MULTI_INDEX;
        startIndex = -1;
        endIndex = -1;
    }

    public enum PRUNE_MODE {
        RANGE, MULTI_INDEX;
    }

    /**
     * do the same thing as RawString.pruneStep, but return itself.
     * WARNING: this method will change this PruneRawString itself.
     */
    @Override
    public PruneRawString pruneStep(int curIndex) {
        pruneMode = PruneRawString.PRUNE_MODE.RANGE;
        startIndex = curIndex;
        endIndex = curIndex + 1;
        return this;
    }

    @Override
    public PruneRawString clone() {
        if (pruneMode == PRUNE_MODE.RANGE) {
            return new PruneRawString(super.getObjList(), pruneMode, startIndex, endIndex, null);
        } else {
            return new PruneRawString(super.getObjList(), pruneMode, -1, -1, (BitSet) indexes.clone());
        }
    }

    public int getSourceSize() {
        return super.size();
    }
}

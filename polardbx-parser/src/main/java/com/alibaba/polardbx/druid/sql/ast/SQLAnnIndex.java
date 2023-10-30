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

package com.alibaba.polardbx.druid.sql.ast;

import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLAnnIndex extends SQLObjectImpl {
    private int indexType;
    private int rtIndexType;
    private Distance distance;

    public SQLAnnIndex clone() {
        SQLAnnIndex x = new SQLAnnIndex();
        x.indexType = indexType;
        x.rtIndexType = rtIndexType;
        x.distance = distance;
        return x;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {

        }
        v.endVisit(this);
    }

    public void setIndexType(String type) {
        this.indexType = IndexType.of(type);
    }

    public void setIndexType(IndexType indexType, boolean state) {
        if (state) {
            this.indexType |= indexType.mask;
        } else {
            this.indexType &= ~indexType.mask;
        }
    }

    public void setRtIndexType(IndexType indexType, boolean state) {
        if (state) {
            this.rtIndexType |= indexType.mask;
        } else {
            this.rtIndexType &= ~indexType.mask;
        }
    }

    public void setRtIndexType(String type) {
        this.rtIndexType = IndexType.of(type);
    }

    public int getIndexType() {
        return indexType;
    }

    public int getRtIndexType() {
        return rtIndexType;
    }

    public Distance getDistance() {
        return distance;
    }

    public void setDistance(Distance distance) {
        this.distance = distance;
    }

    public void setDistance(String distance) {
        if (distance == null) {
            this.distance = null;
            return;
        }

        if (distance.equalsIgnoreCase("Hamming")) {
            this.distance = Distance.Hamming;
        } else if (distance.equalsIgnoreCase("SquaredEuclidean")) {
            this.distance = Distance.SquaredEuclidean;
        } else if (distance.equalsIgnoreCase("DotProduct")) {
            this.distance = Distance.DotProduct;
        }
    }

    public static enum IndexType {
        Flat(1), FastIndex(2);

        public final int mask;

        IndexType(int ordinal) {
            mask = (1 << ordinal);
        }

        private static int of(String type) {
            if (type == null || type.length() == 0) {
                return 0;
            }

            int v = 0;
            String[] items = type.split(",");
            for (String item : items) {
                if (item.trim().equalsIgnoreCase("Flat")) {
                    v |= Flat.mask;
                } else if (item.trim().equalsIgnoreCase("FastIndex")
                    || item.trim().equalsIgnoreCase("FAST_INDEX")) {
                    v |= FastIndex.mask;
                }
            }
            return v;
        }
    }

    public enum Distance {
        Hamming,
        SquaredEuclidean,
        DotProduct // desc
    }
}

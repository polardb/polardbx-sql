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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * raw string built by in target list. Used in x protocol.
 *
 * @author jilong.ljl
 */
public class RawString implements Serializable {

    public static final String RawS = "@@RawS";

    private List objList;
    private transient String raw;

    public RawString(List list) {
        if (list == null) {
            throw GeneralUtil.nestedException("rawstring get empty object list");
        }
        if (!(list instanceof ArrayList)) {
            this.objList = Lists.newArrayList(list);
        } else {
            this.objList = list;
        }
    }

    public Object getObj(int index, int skIndex) {
        if (skIndex == -1) {
            return objList.get(index);
        }

        if (objList.get(index) instanceof List) {
            return ((List<?>) objList.get(index)).get(skIndex);
        }
        throw GeneralUtil.nestedException(
            "rawstring error when get object:" + index + "," + skIndex + "," + buildRawString());
    }

    private List<Object> getObjBySkIndex(int skIndex) {
        if (skIndex == -1) {
            return getObjList();
        }
        List objNew = new ArrayList();
        for (Object o : getObjList()) {
            Preconditions.checkArgument(o instanceof List && ((List<?>) o).size() > skIndex,
                "illegal skIndex:" + skIndex + ",rawstring:" + display());
            Object o1 = ((List<?>) o).get(skIndex);
            objNew.add(o1);
        }
        return objNew;
    }

    public List getObjList() {
        return objList;
    }

    public int size() {
        return objList.size();
    }

    public RawString convertType(Function<Object, Object> function) {
        return convertType(function, -1);
    }

    public RawString convertType(Function<Object, Object> function, int skIndex) {
        List objNew = new ArrayList();
        for (Object o : getObjList()) {
            if (skIndex != -1) {
                Assert.assertTrue(o instanceof List);
                Object o1 = ((List<?>) o).get(skIndex);
                List newList = Lists.newLinkedList();
                newList.addAll((Collection) o);
                newList.set(skIndex, function.apply(o1));
                objNew.add(newList);
            } else {
                objNew.add(function.apply(o));
            }
        }
        return new RawString(objNew);
    }

    public String buildRawString(BitSet x) {
        if (x == null || x.size() == 0) {
            throw GeneralUtil.nestedException("empty param list");
        }
        StringBuilder stringBuilder = new StringBuilder();
        int i = x.nextSetBit(0);
        while (i != -1) {
            if (i >= objList.size()) {
                throw GeneralUtil.nestedException("unmatch obj index in rawstring:" + objList.size() + "," + x);
            }
            Object o = objList.get(i);
            if (o instanceof Number) {
                stringBuilder.append(o).append(",");
            } else if (o instanceof List) {
                stringBuilder.append('(');
                for (Object m : (List) o) {
                    if (m instanceof Number) {
                        stringBuilder.append(m).append(",");
                    } else {
                        stringBuilder.append(objectToRawString(m));
                    }
                }
                stringBuilder.setLength(stringBuilder.length() - 1);
                stringBuilder.append(')').append(',');
            } else {
                stringBuilder.append(objectToRawString(o));
            }
            i = x.nextSetBit(i + 1);
        }
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    public String buildRawString() {
        if (raw != null) {
            return raw;
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (Object o : objList) {
            stringBuilder.append(objectToRawString(o));
        }
        if (stringBuilder.length() >= 1) {
            stringBuilder.setLength(stringBuilder.length() - 1);
        }
        raw = stringBuilder.toString();
        return raw;
    }

    public String objectToRawString(Object o) {
        StringBuilder stringBuilder = new StringBuilder();
        if (o instanceof Number) {
            stringBuilder.append(o).append(",");
        } else if (o instanceof List) {
            if (((List<?>) o).size() == 0) {
                GeneralUtil.nestedException(new IllegalArgumentException("empty list args"));
            }
            stringBuilder.append('(');
            for (Object x : (List) o) {
                stringBuilder.append(objectToRawString(x));
            }
            stringBuilder.setLength(stringBuilder.length() - 1);
            stringBuilder.append(')').append(',');
        } else if (o instanceof byte[]) {
            stringBuilder.append('x').append(TStringUtil.quoteString(TStringUtil.bytesToHexString((byte[]) o)))
                .append(",");
        } else if (o == null) {
            stringBuilder.append("null").append(",");
        } else {
            stringBuilder.append(TStringUtil.quoteString(o.toString())).append(",");
        }
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        // forbidden use RawString directly
        return display();
    }

    public String display() {
        String rs = "Raw(" + buildRawString() + ")";
        if (rs.length() > 4096) {
            return rs.substring(0, 4096) + " ... ";
        }
        return rs;
    }

    /**
     * if subindex == -1 meaning return list object
     * if skindex!=-1 meaning return specify column
     */
    public Object acquireObject(int subIndex, int skIndex) {
        if (subIndex == -1) {
            if (skIndex == -1) {
                return getObjList();
            } else {
                return getObjBySkIndex(skIndex);
            }
        } else {
            return getObj(subIndex, skIndex);
        }
    }

    /**
     * build one PruneRawString with one object(target by curIndex) inside this RawString
     */
    public PruneRawString pruneStep(int curIndex) {
        return new PruneRawString(this.getObjList(), PruneRawString.PRUNE_MODE.RANGE, curIndex, curIndex + 1, null);
    }

    public RawString clone() {
        return new RawString(this.getObjList());
    }
}

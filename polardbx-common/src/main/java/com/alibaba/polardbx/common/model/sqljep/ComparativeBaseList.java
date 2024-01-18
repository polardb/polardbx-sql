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

package com.alibaba.polardbx.common.model.sqljep;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

public abstract class ComparativeBaseList extends Comparative {

    private static final Logger logger = LoggerFactory.getLogger(ComparativeBaseList.class);
    protected List<Comparative> list = new ArrayList<Comparative>(2);

    public ComparativeBaseList(int function, Comparable<?> value) {
        super(function, value);
        list.add(new Comparative(function, value));
    }

    protected ComparativeBaseList() {
        super();
    }

    public ComparativeBaseList(int capacity) {
        super();
        list = new ArrayList<Comparative>(capacity);
    }

    public ComparativeBaseList(Comparative item) {
        super(item.getComparison(), item.getValue());
        list.add(item);
    }

    public List<Comparative> getList() {
        return list;
    }

    public void addComparative(Comparative item) {
        this.list.add(item);
    }

    public ComparativeBaseList clone() {
        try {
            Constructor<? extends ComparativeBaseList> con = this.getClass().getConstructor((Class[]) null);
            ComparativeBaseList compList = con.newInstance((Object[]) null);
            for (Comparative com : list) {
                compList.addComparative(com.clone());
            }
            compList.setComparison(this.getComparison());
            compList.setValue(this.getValue());
            return compList;
        } catch (Exception e) {
            logger.error(e);
            return null;
        }

    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean firstElement = true;
        for (Comparative comp : list) {
            if (!firstElement) {
                sb.append(getRelation());
            }
            sb.append(comp.toString());
            firstElement = false;
        }
        return sb.toString();
    }

    abstract public String getRelation();

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((list == null) ? 0 : list.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ComparativeBaseList other = (ComparativeBaseList) obj;
        if (list == null) {
            if (other.list != null) {
                return false;

            }
        } else if (!list.equals(other.list)) {
            return false;
        }
        return true;
    }

    public void setList(List<Comparative> list) {
        this.list = list;
    }

    @Override
    public void childrenAccept(ComparativeVisitor visitor) {
        for (int i = 0; i < list.size(); i++) {
            visitor.visit(list.get(i), i, this);
        }
    }
}

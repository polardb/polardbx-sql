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

package com.alibaba.polardbx.optimizer.partition;

/**
 * The definition of one (sub)partition
 *
 * @author chenghui.lch
 */
public class SubPartitionSpecTemplate {

    /**
     * the definition of one partition bound info
     */
    protected PartitionBoundSpec boundSpec;


    /**
     * The position of current (sub)partition among all partitions.
     *
     * the first partition is 0, the second partition is 1, ......
     */
    protected int position;

    /**
     * The subpartition name of subpartition template
     */
    protected String partTempName;

    /**
     * The (sub)partition comment
     */
    protected String comment;

    /**
     * The (sub)partition engine type
     */
    protected String engine = "innodb";

    /**
     *  Only for list/list column partition:
     *  flag if contain null value in value list
     */
    protected boolean hasNullValue;

    /**
     *  Only for range/range column partition:
     *  flag if contain max-value in boundValue
     */
    protected boolean isMaxValueRange;

    public SubPartitionSpecTemplate() {
    }

    public SubPartitionSpecTemplate copy() {
        SubPartitionSpecTemplate template = new SubPartitionSpecTemplate();
        template.setPosition(position);
        template.setPartTempName(this.getPartTempName());
        template.setEngine(this.engine);
        template.setComment(this.comment);
        template.setMaxValueRange(this.isMaxValueRange);
        template.setHasNullValue(this.hasNullValue);
        template.setBoundSpec(this.boundSpec.copy());
        return template;
    }

    public PartitionBoundSpec getBoundSpec() {
        return boundSpec;
    }

    public void setBoundSpec(PartitionBoundSpec boundSpec) {
        this.boundSpec = boundSpec;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }


    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engineType) {
        this.engine = engineType;
    }

    public boolean isHasNullValue() {
        return hasNullValue;
    }

    public void setHasNullValue(boolean hasNullValue) {
        this.hasNullValue = hasNullValue;
    }

    public boolean isMaxValueRange() {
        return isMaxValueRange;
    }

    public void setMaxValueRange(boolean maxValueRange) {
        isMaxValueRange = maxValueRange;
    }


    public String getPartTempName() {
        return partTempName;
    }

    public void setPartTempName(String partTempName) {
        this.partTempName = partTempName;
    }
}

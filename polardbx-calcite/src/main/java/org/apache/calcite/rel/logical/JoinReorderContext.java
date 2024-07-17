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

package org.apache.calcite.rel.logical;

public class JoinReorderContext {
    /**
     * left deep
     */
    private boolean hasCommute = false;
    private boolean hasTopPushThrough = false;

    /**
     * zig-zag
     */
    private boolean hasCommuteZigZag = false;

    /**
     * bushy
     */
    private boolean hasExchange = false;
    private boolean hasRightAssociate = false;
    private boolean hasLeftAssociate = false;
    private boolean hasSemiFilter = false;

    public JoinReorderContext() {
    }

    void copyFrom(JoinReorderContext joinReorderContext) {
        this.hasCommute = joinReorderContext.hasCommute;
        this.hasTopPushThrough = joinReorderContext.hasTopPushThrough;
        this.hasExchange = joinReorderContext.hasExchange;
        this.hasLeftAssociate = joinReorderContext.hasLeftAssociate;
        this.hasRightAssociate = joinReorderContext.hasRightAssociate;
        this.hasCommuteZigZag = joinReorderContext.hasCommuteZigZag;
        this.hasSemiFilter = joinReorderContext.hasSemiFilter;
    }

    public void clear() {
        hasCommute = false;
        hasTopPushThrough = false;
        hasCommuteZigZag = false;
        hasExchange = false;
        hasRightAssociate = false;
        hasLeftAssociate = false;
        hasSemiFilter = false;
    }

    public void avoidParticipateInJoinReorder() {
        hasCommute = true;
        hasTopPushThrough = true;
        hasCommuteZigZag = true;
        hasExchange = true;
        hasRightAssociate = true;
        hasLeftAssociate = true;
        hasSemiFilter = true;
    }

    public boolean reordered() {
        return hasCommute
            || hasTopPushThrough
            || hasCommuteZigZag
            || hasExchange
            || hasRightAssociate
            || hasLeftAssociate
            || hasSemiFilter;
    }

    public boolean isHasCommute() {
        return hasCommute;
    }

    public void setHasCommute(boolean hasCommute) {
        this.hasCommute = hasCommute;
    }

    public boolean isHasTopPushThrough() {
        return hasTopPushThrough;
    }

    public void setHasTopPushThrough(boolean hasTopPushThrough) {
        this.hasTopPushThrough = hasTopPushThrough;
    }

    public boolean isHasExchange() {
        return hasExchange;
    }

    public void setHasExchange(boolean hasExchange) {
        this.hasExchange = hasExchange;
    }

    public boolean isHasRightAssociate() {
        return hasRightAssociate;
    }

    public void setHasRightAssociate(boolean hasRightAssociate) {
        this.hasRightAssociate = hasRightAssociate;
    }

    public boolean isHasLeftAssociate() {
        return hasLeftAssociate;
    }

    public void setHasLeftAssociate(boolean hasLeftAssociate) {
        this.hasLeftAssociate = hasLeftAssociate;
    }

    public boolean isHasCommuteZigZag() {
        return hasCommuteZigZag;
    }

    public void setHasCommuteZigZag(boolean hasCommuteZigZag) {
        this.hasCommuteZigZag = hasCommuteZigZag;
    }

    public boolean isHasSemiFilter() {
        return hasSemiFilter;
    }

    public void setHasSemiFilter(boolean hasSemiFilter) {
        this.hasSemiFilter = hasSemiFilter;
    }
}

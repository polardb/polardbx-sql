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

package com.alibaba.polardbx.optimizer.hint.operator;

import java.util.Set;

public class JoinHint {
    private HintType joinType;

    private Set<String> left;

    private Set<String> right;

    public JoinHint(HintType joinType, Set<String> left, Set<String> right) {
        this.joinType = joinType;
        this.left = left;
        this.right = right;
    }

    public HintType getJoinType() {
        return joinType;
    }

    public void setJoinType(HintType joinType) {
        this.joinType = joinType;
    }

    public Set<String> getLeft() {
        return left;
    }

    public void setLeft(Set<String> left) {
        this.left = left;
    }

    public Set<String> getRight() {
        return right;
    }

    public void setRight(Set<String> right) {
        this.right = right;
    }
}
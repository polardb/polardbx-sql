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

package com.alibaba.polardbx.gms.privilege.authorize;

import java.util.Optional;

/**
 * A base implementation for rule. {@inheritDoc}
 *
 * @author bairui.lrj
 * @since 5.4.10
 */
public abstract class AbstractRule implements Rule {
    private Optional<Rule> next;

    protected AbstractRule() {
        this.next = Optional.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNext(Rule next) {
        this.next = Optional.of(next);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Optional<Rule> getNext() {
        return next;
    }
}

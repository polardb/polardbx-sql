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

package com.alibaba.polardbx.gms.node;

import java.io.Serializable;
import java.util.Objects;

public class PolarDBXStatus implements Serializable {

    private boolean highDelayed;
    private boolean highLoaded;

    public PolarDBXStatus() {
    }

    public PolarDBXStatus(boolean highDelayed, boolean highLoaded) {
        this.highDelayed = highDelayed;
        this.highLoaded = highLoaded;
    }

    public boolean isHighDelayed() {
        return highDelayed;
    }

    public void setHighDelayed(boolean highDelayed) {
        this.highDelayed = highDelayed;
    }

    public boolean isHighLoaded() {
        return highLoaded;
    }

    public void setHighLoaded(boolean highLoaded) {
        this.highLoaded = highLoaded;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PolarDBXStatus)) {
            return false;
        }
        PolarDBXStatus status = (PolarDBXStatus) o;
        return highDelayed == status.highDelayed &&
            highLoaded == status.highLoaded;
    }

    @Override
    public int hashCode() {
        return Objects.hash(highDelayed, highLoaded);
    }
}

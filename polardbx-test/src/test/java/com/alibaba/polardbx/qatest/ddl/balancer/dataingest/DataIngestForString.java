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

package com.alibaba.polardbx.qatest.ddl.balancer.dataingest;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author moyi
 * @since 2021/04
 */
public class DataIngestForString extends DataIngest {

    private int rowLength;

    public DataIngestForString(int length) {
        this.rowLength = length;
    }

    @Override
    protected void prepareK(PreparedStatement ps, int k) throws SQLException {
    }

    @Override
    public void generatePk(int dataRows, PreparedStatement ps, int i) throws SQLException {
        String str = randomString(this.rowLength);
        ps.setString(1, str);
        int k = ThreadLocalRandom.current().nextInt(dataRows);
        ps.setInt(2, k);
        this.kSum += k;
    }

    @Override
    protected void checkId() {
        // skip
    }

    private String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char ch = (char) (ThreadLocalRandom.current().nextInt('x' - 'a') + 'a');
            sb.append(ch);
        }
        return sb.toString();
    }
}

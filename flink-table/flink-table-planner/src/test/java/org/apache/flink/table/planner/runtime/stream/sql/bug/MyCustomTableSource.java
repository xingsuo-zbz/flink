/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.runtime.stream.sql.bug;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.util.Random;

public class MyCustomTableSource implements ScanTableSource {
    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "MyCustomTableSource";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return SourceFunctionProvider.of(new MySource(30), false);
    }

    static class MySource implements ParallelSourceFunction<RowData> {
        private static final String[] var = new String[] {"aaa", "bbb", "ccc", "ddd", "eee", "fff"};

        private int windowSize;

        public MySource(int windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            Thread.sleep(5000);
            int size = var.length;
            Random random = new Random();

            for (int i = 0; i < Integer.MAX_VALUE; i++) {
                // 每个窗口期发送1000条数据，定期构造 watermark 的突进
                Thread.sleep(windowSize);
                long time = System.currentTimeMillis() - random.nextInt(windowSize * 2) * 1000;
                if (i % 2000 < 5) {
                    time = System.currentTimeMillis() + windowSize * 2 * 1000;
                }
                GenericRowData rowData = new GenericRowData(3);
                rowData.setField(0, i);
                rowData.setField(1, TimestampData.fromEpochMillis(time));
                rowData.setField(2, StringData.fromString(var[i % size]));
                sourceContext.collectWithTimestamp(rowData, time);
            }
        }

        @Override
        public void cancel() {}
    }
}

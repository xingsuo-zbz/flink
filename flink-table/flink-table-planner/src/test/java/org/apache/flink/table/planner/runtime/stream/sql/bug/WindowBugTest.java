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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowBugTest {

    static StreamExecutionEnvironment environment;
    static StreamTableEnvironment tableEnvironment;
    static int windowSize;

    @BeforeClass
    public static void setUp() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRestartStrategy(RestartStrategies.noRestart());
        environment.setParallelism(2);
        environment.setMaxParallelism(4);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        tableEnvironment = StreamTableEnvironment.create(environment, settings);
        windowSize = 5;
    }

    @Test
    public void testTumbleProcWindowAggWithWatermark() throws Exception {
        String sql1 =
                "create table s1 ("
                        + "id int,"
                        + "event_time timestamp(3),"
                        + "name string,"
                        + "proc_time as PROCTIME(),"
                        + "WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                        + ") with ("
                        + "'connector'='my-source',"
                        + "'window-size'='"
                        + windowSize
                        + "'"
                        + ")";
        tableEnvironment.executeSql(sql1);

        String sql2 =
                "select * from (select\n"
                        + "  name,count(id) as total_count,window_start,window_end\n"
                        + "from\n"
                        + "  TABLE(\n"
                        + "    TUMBLE(\n"
                        + "      TABLE s1,\n"
                        + "      DESCRIPTOR(proc_time),\n"
                        + "      INTERVAL '"
                        + windowSize
                        + "' SECONDS\n"
                        + "    )\n"
                        + "  )\n"
                        + "GROUP BY\n"
                        + "  window_start,\n"
                        + "  window_end,\n"
                        + "  name) where total_count=0";
        tableEnvironment
                .toDataStream(tableEnvironment.sqlQuery(sql2))
                .process(new LogProcessFunction())
                .print();
        environment.execute();
    }

    static class LogProcessFunction extends ProcessFunction<Row, Row> {
        private static final Logger LOG = LoggerFactory.getLogger(LogProcessFunction.class);

        @Override
        public void processElement(
                Row value, ProcessFunction<Row, Row>.Context ctx, Collector<Row> out)
                throws Exception {
            LOG.debug(
                    "currentWatermark {}, value {}.", ctx.timerService().currentWatermark(), value);
            out.collect(value);
        }
    }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter.trident;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.DRPCClient;


public class TridentWordCount {
    public static StormTopology buildTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
                                                    new Values("the man went to the store and bought some candy"),
                                                    new Values("four score and seven years ago"),
                                                    new Values("how many apples can you eat"), new Values("to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        /*stream操作
        * 1分词： each 是filter底层实现. Split算子
        * 2分组：groupby
        * 3保存中间结果：persistentAggregate   MemoryMapState 存储， Count算子 */
        TridentState wordCounts = topology.newStream("spout1", spout).parallelismHint(16).each(new Fields("sentence"),
                                                                                               new Split(), new Fields("word"))
                                          .groupBy(new Fields("word")).persistentAggregate(new MemoryMapState.Factory(),
                                                                                           new Count(), new Fields("count"))
                                          .parallelismHint(16);

        /*DRPCStream 服务，服务名 words
        * 根据传入条件分词
        * groupby 分组
        * stateQuery 查询 wordCounts中缓存的
        * 再次过滤为null
        * Project投影 word, count 字段
        *
        *
        * 后面的需要对应前面的，字段名太多，有点眼花。
        *
        * */
        topology.newDRPCStream("words").each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .project(new Fields("word", "count"));
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(20);
        String topoName = "wordCounter";
        if (args.length > 0) {
            topoName = args[0];
        }
        conf.setNumWorkers(3);
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, buildTopology());
        try (DRPCClient drpc = DRPCClient.getConfiguredClient(conf)) {
            for (int i = 0; i < 10; i++) {
                System.out.println("DRPC RESULT: " + drpc.execute("words", "cat the dog jumped")); // 客户端调用drpc
                Thread.sleep(1000);
            }
        }
    }

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }
}

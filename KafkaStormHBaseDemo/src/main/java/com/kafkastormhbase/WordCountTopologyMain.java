package com.kafkastormhbase;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.kafkastormhbase.bolt.WordCounter;
import com.kafkastormhbase.bolt.WordNormalizer;
import com.kafkastormhbase.spout.WordReader;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-9-22
 * Time: 下午2:21
 * To change this template use File | Settings | File Templates.
 */
public class WordCountTopologyMain {
    public static void main(String[] args) throws InterruptedException {
        //定义一个Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader(),1);
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),2).fieldsGrouping("word-normalizer", new Fields("word"));

        //HBaseMapper mapper = new
        //配置
        Config conf = new Config();
        conf.put("wordsFile", "f:/test.txt");
        conf.setDebug(false);
        //提交Topology
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        //创建一个本地模式cluster
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf,builder.createTopology());
        Utils.sleep(3000);
        cluster.killTopology("Getting-Started-Toplogie");
        cluster.shutdown();
    }
}

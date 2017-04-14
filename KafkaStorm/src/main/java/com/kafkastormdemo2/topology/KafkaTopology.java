package com.kafkastormdemo2.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.testing.TestWordBytesCounter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.kafkastorm.bolt.WordCounter;
import com.kafkastormdemo2.bolt.KafkaWordSplitter;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-9-23
 * Time: 上午9:54
 * To change this template use File | Settings | File Templates.
 */
public class KafkaTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {
        String zks = "192.168.100.72:2181";//zookeeper
        String topic = "test1";
        String zkRoot = "/usr/hadoop/zookeeper";
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkServers = Arrays.asList(new String[]{"192.168.100.72"});
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf));
        builder.setBolt("word-splitter", new KafkaWordSplitter()).shuffleGrouping("kafka-reader");
        builder.setBolt("word-count", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));

        Config conf = new Config();

        String name = KafkaTopology.class.getSimpleName();
        if(args != null && args.length > 0){
            //Nimbus host name passed from command line
            conf.put(Config.NIMBUS_HOST, args[0]);
            conf.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(name, conf, builder.createTopology());
            Thread.sleep(600000);
            cluster.shutdown();
        }
    }
}

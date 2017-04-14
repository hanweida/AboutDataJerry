package com.kafkastormhdfs.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.kafkastormhdfs.bolt.KafkaWordSplitter;
import com.kafkastormhdfs.bolt.WordCounter;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.*;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-9-27
 * Time: 上午10:53
 * To change this template use File | Settings | File Templates.
 */
public class KafkaTopology {

    public static void main(String[] args) throws Exception{
        String zks = "hadoop1:2181";
        String topic = "test1";
        String zRoot ="/usr/hadoop/zookeeper";
        String id = "word";

        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[]{"hadoop1"});
        spoutConfig.zkPort = 2181;

        //Configure HDFS bolt
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("\t");
        // use "\t" instead of "," for field delimiter
        SyncPolicy syncPolicy = new CountSyncPolicy(1);

        //rotate files
        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);
        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/storm/").withPrefix("app_"). withExtension(".log");// set file name format

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://hadoop1:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);


        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConfig));
        builder.setBolt("word-kafka-split", new KafkaWordSplitter()).shuffleGrouping("kafka-reader");
        builder.setBolt("word-count", new WordCounter()).fieldsGrouping("word-kafka-split", new Fields("word"));
        builder.setBolt("hdfs-bolt", hdfsBolt, 1).shuffleGrouping("word-count");

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

package com.kafkastormhbase;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.kafkastormhbase.bolt.KafkaWordSplitter;
import com.kafkastormhbase.bolt.WordCounter;
import com.kafkastormhbase.hbasemapper.MyHbaseMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-10-31
 * Time: 下午3:42
 * To change this template use File | Settings | File Templates.
 */
public class KafkaStormHBase {
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

        /*SimpleHBaseMapper simpleHBaseMapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("word"))
                .withCounterFields(new Fields("count"))
                .withColumnFamily("cf");
*/
        HBaseMapper mapper = new MyHbaseMapper();
        HBaseBolt hBaseBolt = new HBaseBolt("WordCount", mapper).withConfigKey("hbase.conf");



        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConfig));
        builder.setBolt("word-kafka-split", new KafkaWordSplitter()).shuffleGrouping("kafka-reader");
        builder.setBolt("word-count", new WordCounter()).fieldsGrouping("word-kafka-split", new Fields("word"));
        builder.setBolt("HBASE_BOLT", hBaseBolt, 1).shuffleGrouping("word-count");

        Config conf = new Config();
        Map<String, Object> hbconf = new HashMap<String, Object>();
        hbconf.put("hbase.rootdir", "D:\\FlumeKafkaStorm\\StormKafka\\trunk\\FlumeKafkaStorm_version1\\KafkaStormHBase_2\\hbaseConf\\hbase-site.xml");
        conf.put("hbase.conf", hbconf);
        System.out.println(";;;;;;;;;;;;;;;;;;;;;;;;;;" + conf.get("hbase.conf"));
        String name = KafkaStormHBase.class.getSimpleName();
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

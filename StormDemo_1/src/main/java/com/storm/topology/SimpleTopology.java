package com.storm.topology;

import com.storm.spout.SimpleSpout;
import com.storm.bolt.SimpleBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-9-22
 * Time: 上午11:43
 * To change this template use File | Settings | File Templates.
 */
public class SimpleTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SimpleSpout(),1);
        builder.setBolt("bolt", new SimpleBolt(),1).shuffleGrouping("spout");
        Config conf = new Config();
        conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("firstTopo", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("firstTopo");
            cluster.shutdown();
        }
    }
}

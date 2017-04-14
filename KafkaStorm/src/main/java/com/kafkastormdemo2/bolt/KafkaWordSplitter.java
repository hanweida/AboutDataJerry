package com.kafkastormdemo2.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-9-23
 * Time: 上午10:25
 * To change this template use File | Settings | File Templates.
 */
public class KafkaWordSplitter extends BaseRichBolt {
    //private static final Logger LOG = LoggerFactory.getLogger(KafkaWordSplitter.class);
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        //To change body of implemented methods use File | Settings | File Templates.
        this.collector = outputCollector;
    }

    public void execute(Tuple tuple) {
        //To change body of implemented methods use File | Settings | File Templates.
        String line = tuple.getString(0);
        System.out.println("line++++++++++++++++++++++++++++++++" + line);
        String[] words = line.split("\\s+");
        synchronized (collector){
            for(String word : words){
                collector.emit(tuple, new Values(word, 1));
            }
        }
        synchronized (collector){
            collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}

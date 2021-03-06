package com.kafkastormhbase.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-9-27
 * Time: 上午10:52
 * To change this template use File | Settings | File Templates.
 */
public class WordCounter extends BaseRichBolt {

    private OutputCollector collector;

    private Map<String, AtomicInteger> countMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.countMap = new HashMap<String, AtomicInteger>();
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);

        collector.emit(tuple, new Values(word, 1));
        collector.ack(tuple);
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
        //To change body of implemented methods use File | Settings | File Templates.
    }
}

package com.kafkastorm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-9-23
 * Time: 上午10:42
 * To change this template use File | Settings | File Templates.
 */
public class WordCounter extends BaseRichBolt {

    //private static final Logger LOG = LoggerFactory.getLogger(WordCounter.class);
    private OutputCollector collector;
    private Map<String, AtomicInteger> counterMap;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counterMap = new HashMap<String, AtomicInteger>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        int count = tuple.getInteger(1);
        AtomicInteger ai = this.counterMap.get(word);
        if(ai == null){
            ai = new AtomicInteger();
            this.counterMap.put(word, ai);
        }
        ai.addAndGet(count);
        synchronized (collector){
            collector.ack(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //To change body of implemented methods use File | Settings | File Templates.
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
        //LOG.info("The final result :");
        Iterator<Map.Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry<String, AtomicInteger> entry = iter.next();
        }
    }
}

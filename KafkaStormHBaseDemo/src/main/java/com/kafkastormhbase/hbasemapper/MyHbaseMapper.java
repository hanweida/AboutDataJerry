package com.kafkastormhbase.hbasemapper;

import backtype.storm.tuple.Tuple;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.common.ColumnList;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-10-31
 * Time: 下午5:56
 * To change this template use File | Settings | File Templates.
 */
public class MyHbaseMapper implements HBaseMapper {
    public byte[] rowKey(Tuple tuple) {
        //根据tuple指定id作为rowkey
        return tuple.getStringByField("word").getBytes();  //To change body of implemented methods use File | Settings | File Templates.
    }

    public ColumnList columns(Tuple tuple) {
        ColumnList cols = new ColumnList();
        //参数依次是 列族名、列名、值
        cols.addColumn("c1".getBytes(), "val".getBytes(), tuple.getStringByField("word").getBytes());
        //cols.addColumn("c2".getBytes(), "num".getBytes(), tuple.getStringByField("num").getBytes());
        return cols;  //To change body of implemented methods use File | Settings | File Templates.
    }


}

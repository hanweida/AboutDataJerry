package com.storm.starter.test;

import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.commons.collections.map.HashedMap;
import org.apache.thrift7.TException;

import java.util.Map;

/**
 * 测试DRPC，BasicDRPCTopology类，获得结果
 * Created by ES-BF-IT-126 on 2017/4/20.
 */
public class TestDRPC {
    public static void main(String[] args){
        Map map = new HashedMap();
        Config conf = new Config();
        DRPCClient client = new DRPCClient("hadoop1", 3772);
        String result = null;
        try {
            result = client.execute("exclamation", "Hello Success");
        } catch (TException e) {
            e.printStackTrace();
        } catch (DRPCExecutionException e) {
            e.printStackTrace();
        }
        System.out.println(result);
    }
}

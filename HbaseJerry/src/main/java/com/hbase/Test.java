package com.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: ES-BF-IT-126
 * Date: 16-10-20
 * Time: 下午6:01
 * To change this template use File | Settings | File Templates.
 */
public class Test {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin hBaseAdmin = connection.getAdmin();
        String tableName = "t_media_income_admin";
        Test test = new Test();
        /**
         * 增加表
         */
        //test.addTable(hBaseAdmin, tableName);
        /**
         * 列出所有的表
         */
        test.getAllTables(hBaseAdmin);
        /**
         * 删除表
         */
        //test.deleteTables(hBaseAdmin, tableName);
        //test.getAllTables(hBaseAdmin);
        /**
         * 是否存在表
         */
        //test.existsTable(hBaseAdmin, tableName);
        /**
         * 添加数据
         *
         */
        //test.putDatas(connection, tableName);
        /**
         * 删除数据
         */
        /**
         * 修改数据
         */
        /**
         * 查询数据
         */
        test.selectDatas(connection, tableName);
    }
    // Hbase获取所有的表信息
    public List getAllTables(Admin admin) {
        List<String> tables = null;
        if (admin != null) {
            try {
                HTableDescriptor[] allTable = admin.listTables();
                if (allTable.length > 0)
                    tables = new ArrayList<String>();
                for (HTableDescriptor hTableDescriptor : allTable) {
                    tables.add(hTableDescriptor.getNameAsString());
                    System.out.println(hTableDescriptor.getNameAsString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return tables;
    }

    /**
     * 添加表
     * @param hBaseAdmin
     * @param table
     * @throws IOException
     */
    public void addTable(Admin hBaseAdmin, String table) throws IOException{
        TableName tableName = TableName.valueOf(table);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
        hTableDescriptor.addFamily(hColumnDescriptor.setCompressionType(Compression.Algorithm.NONE));
        if(!existsTable(hBaseAdmin, table)){
            hBaseAdmin.createTable(hTableDescriptor);
        }
    }

    /**
     * 删除表
     * @param hBaseAdmin
     * @param tableName
     * @throws IOException
     */
    public void deleteTables(HBaseAdmin hBaseAdmin, String tableName) throws IOException{
        hBaseAdmin.disableTable(tableName);
        hBaseAdmin.deleteTable(tableName);
    }

    /**
     * 删除表通过 TableName 类
     * @param hBaseAdmin
     * @param tableName
     * @throws IOException
     */
    public void deleteTableByTableName(HBaseAdmin hBaseAdmin, String tableName) throws IOException{
        TableName tableName1 = TableName.valueOf(tableName);
        hBaseAdmin.disableTable(tableName1);
        hBaseAdmin.deleteTable(tableName1);
    }

    /**
     * 是否存在表
     * @param hBaseAdmin
     * @param tableName
     * @throws IOException
     */
    public boolean existsTable(Admin hBaseAdmin, String tableName) throws IOException{
        boolean isExists = hBaseAdmin.tableExists(TableName.valueOf(tableName));
        System.out.println("is existsTable" + isExists);
        return isExists;
    }

    /**
     * 添加数据
     * @param hConnection
     * @param tableName
     */
    public void putDatas(Connection hConnection, String tableName) throws IOException{

        String [] rows = {"0000_000_00000","1111_111_11111"};
        String [] column = {"userid", "mmid", "date"};
        String [][] values = {{"0000", "0000", "00000000"}, {"1111", "111", "11111111"}};
        TableName tableName1 = TableName.valueOf(tableName);
        byte [] family = Bytes.toBytes("date");
        Table Table = hConnection.getTable(tableName1);
        for(int i = 0 ;i < rows.length ; i++){
            System.out.println("==============" + rows[i]);
            byte [] rowkey = rows[i].getBytes();
            Put put = new Put(rowkey);
            for(int j = 0 ; j < column.length; j++){
                byte [] qualifier = Bytes.toBytes(column[j]);
                byte [] value = Bytes.toBytes(values[i][j]);
                put.add(family, qualifier, value);
            }
            Table.put(put);
        }
        Table.close();
    }

    /**
     * 查询数据
     * @param hConnection
     * @param tableName
     * @throws IOException
     */
    public void selectDatas(Connection hConnection, String tableName) throws IOException{
        TableName tableName1 = TableName.valueOf(tableName);
        Table table = hConnection.getTable(tableName1);
        Get get = new Get("20161031_2_1_1_4560_1299".getBytes());
        Result result = table.get(get);
        List list = result.listCells();
        List<Cell> listCell =  result.getColumnCells("info".getBytes(),"shownum".getBytes());
        System.out.println(Bytes.toString(result.getValue("info".getBytes(),"shownum".getBytes())));
        //System.out.println(listCell.get(0));
    }
}


package com.dolen.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {
        //1.建立连接器
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("172.20.10.39", 11111),"example","","" );
        //2.
        while (true){
            connector.connect();

            connector.subscribe("test.*");//监控canal_test库
            Message message = connector.get(100);//监控100个sql

            if(message.getEntries().size()==0){
                System.out.println("当没有数据，休息5秒");
                try {
                    //当没有数据，休息5秒
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //处理每一条sql
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //判断sql事件为：ROWDATA 主要用于标识事务的开始，变更数据，结束*
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){

                        CanalEntry.RowChange rowChange = null;
                        try {
                            //反序列化
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                        //获取数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                        //获取事件
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //获取表名,在序列化之前获取表名字
                        String tableName = entry.getHeader().getTableName();

                        //调用处理方法：
                        CanalHandler canalHandler = new CanalHandler(tableName, eventType, rowDatasList);

                        canalHandler.handle();
                    }
                }
            }
        }
    }

}

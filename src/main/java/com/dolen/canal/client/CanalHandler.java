package com.dolen.canal.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.dolen.canal.util.MyKafkaSender;

import java.util.List;
import java.util.Map;

//业务类
public class CanalHandler {

    //表名字
    String tableName ;

    //事件
    CanalEntry.EventType eventType;

    //数据
    List<CanalEntry.RowData> rowDatasList;


    public CanalHandler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDatasList = rowDatasList;
    }


    //insert处理方法
    public void handle(){
        if(tableName.equals("map_data_sample")&&eventType.equals(CanalEntry.EventType.INSERT)){
            //获取rowData
            for (CanalEntry.RowData rowData : rowDatasList) {
                  sendKafka(rowData,CanalConstans.KAFKA_TOPIC_TEST);//测试主题
                }
        }
        if(tableName.equals("map_data_sample")&&eventType.equals(CanalEntry.EventType.UPDATE)){
            //获取rowData
            for (CanalEntry.RowData rowData : rowDatasList) {
                  sendKafka(rowData,CanalConstans.KAFKA_TOPIC_TEST);//测试主题
                }
        }
    }
    private void sendKafka(CanalEntry.RowData rowData,String topic){
        //获取列
        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
        JSONObject jsonObject = new JSONObject();

        //获取每列值
        for (CanalEntry.Column column : afterColumnsList) {

            System.out.println(column.getName() + "===" + column.getValue());
            //发送数据到topic中
            jsonObject.put(column.getName(), column.getValue());
        }
        String rowJson = jsonObject.toJSONString();//转成json

        MyKafkaSender.send(topic, rowJson);//发送
    }


}

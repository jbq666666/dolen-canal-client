package com.dolen.canal.client;

import java.text.SimpleDateFormat;
import java.util.Date;

public class testMain {
    public static void main(String[] args) {
        System.out.println(new Date().getTime());//date 转换成long
        long longTime =1574062267879L;

        System.out.println(new Date(1574062267879L));//long 转换成
      //  new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(1574062267879L), 1, 1))
    }
}

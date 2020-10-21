package com.Kafka2Spark;

import java.util.Arrays;

public class Test {

    public static void main(String[] args){

        String[] stock_type_rule = {"0","1","d","c","h","e","g","D","L","6","T"};//证券类别
        System.out.println("************************ " +Arrays.toString(stock_type_rule)+ "**************************");

        String stock_type_rule2 = "0,1,d,c,h";
        System.out.println("************************ " +stock_type_rule2+ "**************************");

        String[] stock_type_rule3 = stock_type_rule2.split(",");
        System.out.println("************************ " +Arrays.toString(stock_type_rule3)+ "**************************");

    }
}



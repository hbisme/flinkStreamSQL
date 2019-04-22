package com.dtstack.flink.sql.myTest;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class TypeInformationTest {
    public static void main(String[] args) {
        TypeInformation<String> info = TypeInformation.of(String.class);
        System.out.println(info);

        ArrayList<String> list = new ArrayList<String>();
        list.add("line1");
        list.add("line2");
        TypeInformation<ArrayList<String>> info1 = TypeInformation.of(new TypeHint<ArrayList<String>>() {
        });

        TypeInformation<Tuple2<String, Double>> info3 = TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {
        });






    }
}

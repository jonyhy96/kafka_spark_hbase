package cn.jony.spark.service.Impl;


import cn.jony.spark.model.Location;
import cn.jony.spark.service.HbaseInsert;

public class HbaseInsertImpl implements HbaseInsert {
    @Override
    public void insert(Location location) {
        System.out.println("insert ----------------"+location.getPlateNum());
    }
}

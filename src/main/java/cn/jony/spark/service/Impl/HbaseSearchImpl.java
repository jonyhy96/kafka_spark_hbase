package cn.jony.spark.service.Impl;


import cn.jony.spark.model.Location;
import cn.jony.spark.service.HbaseSearch;

public class HbaseSearchImpl implements HbaseSearch {
    @Override
    public Location search(Location location) {
        System.out.println("search -------------"+location.getFrom());
        return null;
    }
}

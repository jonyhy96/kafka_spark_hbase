package cn.jony.spark.service;

import cn.jony.spark.model.Location;

public interface HbaseInsert {
    void insert(Location location);
}

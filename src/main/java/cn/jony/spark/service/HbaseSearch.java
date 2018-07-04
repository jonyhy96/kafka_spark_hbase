package cn.jony.spark.service;

import cn.jony.spark.model.Location;

public interface HbaseSearch {
    Location search(Location location);
}

package cn.jony.spark.service;


import cn.jony.spark.model.Location;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class HbasePutConvertFunction implements PairFunction<Location, ImmutableBytesWritable, Put> {
    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Location location) throws Exception {
        Put put = new Put(Bytes.toBytes(location.getPlateNum()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("plateNum"), Bytes.toBytes(location.getPlateNum()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("from"), Bytes.toBytes(location.getFrom()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("course"), Bytes.toBytes(location.getCourse()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("imei"), Bytes.toBytes(location.getImei()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("lat"), Bytes.toBytes(location.getLat()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("lng"), Bytes.toBytes(location.getLng()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("speed"), Bytes.toBytes(location.getSpeed()));
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("time"), Bytes.toBytes(location.getTime()));
        return new Tuple2<>(new ImmutableBytesWritable(), put);
    }
}

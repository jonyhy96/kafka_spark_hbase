package cn.jony.spark.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;

public class HbaseSaveFunction implements VoidFunction<JavaPairRDD<ImmutableBytesWritable, Put>> {

    final private  Configuration hbaseconf;
    final private Configuration jobConf;
    public HbaseSaveFunction() {
        hbaseconf = HBaseConfiguration.create();
        hbaseconf.set(TableOutputFormat.OUTPUT_TABLE, "location");
        hbaseconf.set("hbase.zookeeper.quorum", "192.168.31.41");
        hbaseconf.set("hbase.zookeeper.property.clientPort","2181");
        jobConf  = new Configuration(hbaseconf);
        jobConf.set("mapreduce.job.output.key.class", "Text");
        jobConf.set("mapreduce.job.output.value.class", "Text");
        jobConf.set("mapreduce.outputformat.class", "org.apache.hadoop.hbase.mapreduce.TableOutputFormat");
    }

    @Override
    public void call(JavaPairRDD<ImmutableBytesWritable, Put> immutableBytesWritablePutJavaPairRDD) throws Exception {
        immutableBytesWritablePutJavaPairRDD.saveAsNewAPIHadoopDataset(jobConf);
    }
}

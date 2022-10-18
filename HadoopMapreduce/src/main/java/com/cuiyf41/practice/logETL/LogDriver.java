package com.cuiyf41.practice.logETL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
//        args = new String[] { "F:\\data\\log\\journal.log", "F:\\data\\output" };

        // 1 获取job信息
        Configuration conf = new Configuration();

        // 指定mapreduce程序运行的hdfs的相关运行参数
        conf.set("fs.defaultFS", "hdfs://hadoop000:9000");
        conf.set("dfs.client.use.datanode.hostname", "true");
        System.setProperty("HADOOP_USER_NAME", "root");

        Job job = Job.getInstance(conf);

        // 2 加载jar包
        job.setJarByClass(LogDriver.class);

        // 3 关联map
        job.setMapperClass(LogMapper.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置reducetask个数为0
        job.setNumReduceTasks(0);

        // 5 设置输入和输出路径
//        Path input = new Path(args[0]);
//        Path output = new Path(args[1]);

        // 指定该 mapreduce 程序数据的输入和输出路径
        Path input = new Path("/input");
        Path output = new Path("/output");

        // 如果输出路径存在，则进行删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output,true);
        }
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // 6 提交
        job.waitForCompletion(true);
    }
}

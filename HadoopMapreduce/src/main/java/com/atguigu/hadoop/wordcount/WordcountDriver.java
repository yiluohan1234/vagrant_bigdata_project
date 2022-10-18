package com.atguigu.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordcountDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "/Users/cuiyufei/data/input", "/Users/cuiyufei/data/output" };

        // 1 获取配置信息以及封装任务
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置jar加载路径
        job.setJarByClass(WordcountDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 如果不设置InputFormat，它默认用的是TextInputFormat.class
//        job.setInputFormatClass(CombineTextInputFormat.class);

        //虚拟存储切片最大值设置4m
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        // 如果输出路径存在，则进行删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output,true);
        }
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // 方案一：指定需要使用combiner，以及用哪个类作为combiner的逻辑
        job.setCombinerClass(WordcountCombiner.class);
        // 方案二：指定需要使用Combiner，以及用哪个类作为Combiner的逻辑
//        job.setCombinerClass(WordcountReducer.class);
        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);


    }
}


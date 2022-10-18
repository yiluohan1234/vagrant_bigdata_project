package com.atguigu.practice.logETL;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    // 用来存储网站url分类数据
    Set<String> pages = new HashSet<String>();
    Text k = new Text();
    NullWritable v = NullWritable.get();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pages.add("/images/my.jpg");
        pages.add("/js/google.js");
        pages.add("/js/baidu.js");
        pages.add("/feed/");
        pages.add("/favicon.ico");
        pages.add("/images");

        pages.add("/");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取1行
        String line = value.toString();

        // 2 解析日志是否合法
        LogBean bean = LogParser.parseLog(line);
//        // 过滤js/图片/css等静态资源
//        LogParser.filtStaticResource(bean, pages);

        if (!bean.isValid()) {
            return;
        }

        k.set(bean.toString());

        // 3 输出
        context.write(k, v);


    }

}

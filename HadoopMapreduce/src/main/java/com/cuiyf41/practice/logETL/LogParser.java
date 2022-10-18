package com.cuiyf41.practice.logETL;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Set;

/**
 * @ClassName LogParser
 * @Description 解析日志信息
 * @Author yiluohan1234
 * @Date 2022/10/10
 * @Version V1.0
 */
public class LogParser {
    //定义时间格式
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

    // 解析日志
    public static LogBean parseLog(String line) {

        LogBean logBean = new LogBean();

        // 1 截取
        String[] fields = line.split(" ");

        //如果数组长度小于等于11，说明这条数据不完整，因此可以忽略这条数据
        if (fields.length > 11) {

            // 2封装数据
            logBean.setRemote_addr(fields[0]);
            logBean.setRemote_user(fields[1]);

            String time_local = formatDate(fields[3].substring(1));
            if(null == time_local || "".equals(time_local))
                time_local="-invalid_time-";
            logBean.setTime_local(time_local);

            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setBody_bytes_sent(fields[9]);
            logBean.setHttp_referer(fields[10]);

            // 如果useragent元素较多，拼接useragent
            if (fields.length > 12) {
                // logBean.setHttp_user_agent(fields[11] + " "+ fields[12]);
                StringBuilder sb = new StringBuilder();
                for(int i=11; i<fields.length; i++){
                    sb.append(fields[i]);
                }
                logBean.setHttp_user_agent(sb.toString());
            }else {
                logBean.setHttp_user_agent(fields[11]);
            }

            // 大于400，HTTP错误
            if (Integer.parseInt(logBean.getStatus()) >= 400) {
                logBean.setValid(false);
            }
            if("-invalid_time-".equals(logBean.getTime_local())){
                logBean.setValid(false);
            }
        }else {
            logBean.setValid(false);
        }

        return logBean;
    }

    //添加标识
    public static void filtStaticResource(LogBean bean, Set<String> pages) {
        if (!pages.contains(bean.getRequest())) {
            bean.setValid(false);
        }
    }

    //格式化时间方法
    public static String formatDate(String time_local) {
        try {
            return df2.format(df1.parse(time_local));
        } catch (ParseException e) {
            return null;
        }
    }

}

package coupon;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task3_CouponFilterAndSort {

    public static class FilterMapper 
        extends Mapper<Object, Text, Text, Text>{
        
        // 用于存储所有优惠券的使用信息
        private Map<String, String> couponMap = new HashMap<>();
        private int totalUsage = 0;
        
        public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
    
            String line = value.toString();
            String[] parts = line.split("\t");
            
            if (parts.length < 2) return;
            
            String couponId = parts[0];
            String[] values = parts[1].split(",");
            int usageCount = Integer.parseInt(values[0]);
            long totalInterval = Long.parseLong(values[1]);
            
            // 存储优惠券信息
            couponMap.put(couponId, usageCount + "," + totalInterval);
            totalUsage += usageCount;
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 计算阈值：总使用次数的1%
            double threshold = totalUsage * 0.01;
            
            // 筛选并输出符合条件的优惠券
            for (Map.Entry<String, String> entry : couponMap.entrySet()) {
                String couponId = entry.getKey();
                String[] values = entry.getValue().split(",");
                int usageCount = Integer.parseInt(values[0]);
                long totalInterval = Long.parseLong(values[1]);
                
                if (usageCount > threshold) {
                    double avgInterval = (double) totalInterval / usageCount;
                    // 使用平均间隔作为键进行排序
                    String outputKey = String.format("%.2f", avgInterval) + "_" + couponId;
                    context.write(new Text(outputKey), new Text(couponId + "\t" + avgInterval));
                }
            }
        }
    }
    
    public static class SortReducer 
        extends Reducer<Text, Text, Text, DoubleWritable> {
        
        private DoubleWritable result = new DoubleWritable();
        
        public void reduce(Text key, Iterable<Text> values, 
                          Context context
                          ) throws IOException, InterruptedException {
            
            for (Text val : values) {
                String[] parts = val.toString().split("\t");
                String couponId = parts[0];
                double avgInterval = Double.parseDouble(parts[1]);
                
                result.set(avgInterval);
                context.write(new Text(couponId), result);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "coupon filter and sort");
        job.setJarByClass(Task3_CouponFilterAndSort.class);
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
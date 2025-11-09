package coupon;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task2_MerchantDistanceAnalysis {

    /**
     * Mapper类
     * 输入：原始数据行
     * 输出：键为"商家ID_距离"，值为用户ID
     */
    public static class DistanceMapper 
        extends Mapper<Object, Text, Text, Text>{
        
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        
        public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
    
            String line = value.toString();
    
            // 跳过表头
            if (line.startsWith("User_id")) {
                return;
            }
    
            // 使用逗号分割，保留空字段
            String[] fields = line.split(",", -1);
            if (fields.length < 6) return;
    
            String userId = fields[0].trim();
            String merchantId = fields[1].trim();
            String distance = fields[4].trim(); // Distance字段在索引4
    
            // 过滤无效数据
            if (userId.isEmpty() || userId.equalsIgnoreCase("NULL") ||
                merchantId.isEmpty() || merchantId.equalsIgnoreCase("NULL")) {
                return;
            }
            
            // 处理距离字段：空值转为"NULL"
            if (distance.isEmpty()) {
                distance = "NULL";
            }
    
            // 输出格式: 商家ID_距离 -> 用户ID
            outputKey.set(merchantId + "_" + distance);
            outputValue.set(userId);
            context.write(outputKey, outputValue);
        }
    }
    
    /**
     * Reducer类
     * 输入：同一个(商家ID_距离)下的所有用户ID
     * 输出：商家ID_距离和对应的唯一用户数量
     */
    public static class DistanceReducer 
        extends Reducer<Text, Text, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<Text> values, 
                          Context context
                          ) throws IOException, InterruptedException {
            
            // 使用Set对用户ID去重
            Set<String> uniqueUsers = new HashSet<>();
            
            for (Text val : values) {
                uniqueUsers.add(val.toString());
            }
            
            // 输出去重后的用户数量
            result.set(uniqueUsers.size());
            context.write(key, result);
        }
    }
    
    /**
     * Combiner类 - 局部聚合优化
     * 在Map端进行初步去重，减少网络传输
     */
    public static class DistanceCombiner 
        extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, 
                          Context context
                          ) throws IOException, InterruptedException {
            
            Set<String> uniqueUsers = new HashSet<>();
            
            for (Text val : values) {
                uniqueUsers.add(val.toString());
            }
            
            // 为每个唯一用户输出一次，在Reducer端会再次聚合
            for (String user : uniqueUsers) {
                context.write(key, new Text(user));
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "merchant distance analysis");
        
        job.setJarByClass(Task2_MerchantDistanceAnalysis.class);
        job.setMapperClass(DistanceMapper.class);
        job.setCombinerClass(DistanceCombiner.class);
        job.setReducerClass(DistanceReducer.class);
        
        // 设置输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
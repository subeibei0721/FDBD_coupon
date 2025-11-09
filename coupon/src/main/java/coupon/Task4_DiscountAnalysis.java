package coupon;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task4_DiscountAnalysis {

    public static class DiscountMapper 
        extends Mapper<Object, Text, Text, Text>{
        
        private Text discountType = new Text();
        private Text usageInfo = new Text();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        
        public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
    
            String line = value.toString();
    
            // 跳过表头
            if (line.startsWith("User_id")) {
                return;
            }
    
            String[] fields = line.split(",", -1);
            if (fields.length < 7) return;
    
            String couponId = fields[2].trim();
            String discountRate = fields[3].trim();
            String dateReceived = fields[5].trim();
            String dateUsed = fields[6].trim();
    
            // 只处理有优惠券的记录
            if (couponId.isEmpty() || couponId.equalsIgnoreCase("NULL") ||
                discountRate.isEmpty() || discountRate.equalsIgnoreCase("NULL")) {
                return;
            }
    
            // 判断折扣类型
            String type;
            if (discountRate.equals("fixed")) {
                type = "fixed";
            } else if (discountRate.contains(":")) {
                type = "manjian"; // 满减
            } else {
                type = "direct";  // 直接折扣
            }
    
            // 判断是否使用
            int used = 0;
            long interval = 0;
            
            if (!dateUsed.isEmpty() && !dateUsed.equalsIgnoreCase("NULL") &&
                !dateReceived.isEmpty() && !dateReceived.equalsIgnoreCase("NULL")) {
                used = 1;
                
                try {
                    // 计算使用间隔天数
                    Date receivedDate = dateFormat.parse(dateReceived);
                    Date usedDate = dateFormat.parse(dateUsed);
                    interval = (usedDate.getTime() - receivedDate.getTime()) / (1000 * 60 * 60 * 24);
                } catch (ParseException e) {
                    // 日期解析失败，间隔设为0
                    interval = 0;
                }
            }
            
            // 输出：折扣类型 -> "领取数,使用数,间隔天数"
            discountType.set(type);
            usageInfo.set("1," + used + "," + interval);
            context.write(discountType, usageInfo);
        }
    }
    
    public static class DiscountReducer 
        extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();
        
        public void reduce(Text key, Iterable<Text> values, 
                          Context context
                          ) throws IOException, InterruptedException {
            
            long totalReceived = 0;    // 总领取数
            long totalUsed = 0;        // 总使用数
            long totalInterval = 0;    // 总间隔天数
            
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalReceived += Long.parseLong(parts[0]);
                totalUsed += Long.parseLong(parts[1]);
                totalInterval += Long.parseLong(parts[2]);
            }
            
            // 计算使用率和平均间隔
            double usageRate = totalReceived > 0 ? (double) totalUsed / totalReceived : 0.0;
            double avgInterval = totalUsed > 0 ? (double) totalInterval / totalUsed : 0.0;
            
            // 格式化输出：总领取数 TAB 使用数 TAB 使用率 TAB 平均间隔
            String output = String.format("%d\t%d\t%.4f\t%.2f", 
                                         totalReceived, totalUsed, usageRate, avgInterval);
            result.set(output);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "discount rate analysis");
        job.setJarByClass(Task4_DiscountAnalysis.class);
        job.setMapperClass(DiscountMapper.class);
        job.setReducerClass(DiscountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
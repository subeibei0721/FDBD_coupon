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

public class Task4_DiscountRateAnalysis {

    public static class DiscountStrengthMapper 
        extends Mapper<Object, Text, Text, Text>{
        
        private Text discountGroup = new Text();
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
    
            // 计算折扣力度并分组
            String group = getDiscountGroup(discountRate);
            if (group == null) return;
    
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
            
            // 输出：折扣分组 -> "领取数,使用数,间隔天数"
            discountGroup.set(group);
            usageInfo.set("1," + used + "," + interval);
            context.write(discountGroup, usageInfo);
        }
        
        // 根据折扣率计算折扣分组
        private String getDiscountGroup(String discountRate) {
            if (discountRate.equals("fixed")) {
                return "fixed";
            } else if (discountRate.contains(":")) {
                // 满减优惠：计算折扣力度
                String[] parts = discountRate.split(":");
                if (parts.length < 2) return null;
                
                try {
                    double fullAmount = Double.parseDouble(parts[0]);
                    double discountAmount = Double.parseDouble(parts[1]);
                    double discountStrength = discountAmount / fullAmount;
                    
                    // 满减分组
                    if (discountStrength < 0.05) return "manjian_0-5%";
                    else if (discountStrength < 0.1) return "manjian_5-10%";
                    else if (discountStrength < 0.2) return "manjian_10-20%";
                    else if (discountStrength < 0.3) return "manjian_20-30%";
                    else return "manjian_30%+";
                } catch (NumberFormatException e) {
                    return null;
                }
            } else {
                // 直接折扣
                try {
                    double discount = Double.parseDouble(discountRate);
                    double discountStrength = 1 - discount; // 折扣力度 = 1 - 折扣率
                    
                    // 直接折扣分组
                    if (discountStrength < 0.05) return "direct_0-5%";
                    else if (discountStrength < 0.1) return "direct_5-10%";
                    else if (discountStrength < 0.2) return "direct_10-20%";
                    else if (discountStrength < 0.3) return "direct_20-30%";
                    else return "direct_30%+";
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
    }
    
    public static class DiscountStrengthReducer 
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
        Job job = Job.getInstance(conf, "discount strength analysis");
        job.setJarByClass(Task4_DiscountRateAnalysis.class);
        job.setMapperClass(DiscountStrengthMapper.class);
        job.setReducerClass(DiscountStrengthReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

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

public class Task3_CouponTimeAnalysis {

    public static class CouponMapper 
        extends Mapper<Object, Text, Text, Text>{
        
        private Text couponId = new Text();
        private Text intervalInfo = new Text();
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
    
            String cId = fields[2].trim();
            String dateReceived = fields[5].trim();
            String dateUsed = fields[6].trim();
    
            // 只处理使用过的优惠券：Coupon_id不为空，Date_received和Date都不为空
            if (cId.isEmpty() || cId.equalsIgnoreCase("NULL") ||
                dateReceived.isEmpty() || dateReceived.equalsIgnoreCase("NULL") ||
                dateUsed.isEmpty() || dateUsed.equalsIgnoreCase("NULL")) {
                return;
            }
    
            try {
                // 计算使用间隔天数
                Date receivedDate = dateFormat.parse(dateReceived);
                Date usedDate = dateFormat.parse(dateUsed);
                long interval = (usedDate.getTime() - receivedDate.getTime()) / (1000 * 60 * 60 * 24);
                
                // 输出：优惠券ID -> "1,间隔天数" (1表示一次使用，间隔天数用于后续求平均)
                couponId.set(cId);
                intervalInfo.set("1," + interval);
                context.write(couponId, intervalInfo);
                
            } catch (ParseException e) {
                // 日期解析失败，跳过该记录
                return;
            }
        }
    }
    
    public static class CouponReducer 
        extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();
        
        public void reduce(Text key, Iterable<Text> values, 
                          Context context
                          ) throws IOException, InterruptedException {
            
            int usageCount = 0;
            long totalInterval = 0;
            
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                usageCount += Integer.parseInt(parts[0]);
                totalInterval += Long.parseLong(parts[1]);
            }
            
            // 输出：优惠券ID 使用次数,总间隔天数
            result.set(usageCount + "," + totalInterval);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "coupon time analysis");
        job.setJarByClass(Task3_CouponTimeAnalysis.class);
        job.setMapperClass(CouponMapper.class);
        job.setReducerClass(CouponReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
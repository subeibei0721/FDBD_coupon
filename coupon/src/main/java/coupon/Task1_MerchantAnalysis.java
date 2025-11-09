package coupon;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1_MerchantAnalysis {

    public static class TokenizerMapper 
        extends Mapper<Object, Text, Text, Text>{
        
        private Text merchantId = new Text();
        private Text type = new Text();
        
        public void map(Object key, Text value, Context context
               ) throws IOException, InterruptedException {
    
            String line = value.toString();
    
            // 跳过表头
            if (line.startsWith("User_id")) {
                return;
            }
    
            // 使用逗号分割，保留空字段
            String[] fields = line.split(",", -1);
            if (fields.length < 7) return;
    
            String mId = fields[1].trim();
            String couponId = fields[2].trim();
            String dateUsed = fields[6].trim();
    
            // 过滤无效商家ID
            if (mId.isEmpty() || mId.equalsIgnoreCase("NULL")) {
                return;
            }
    
            // 判断是否有优惠券、是否消费
            boolean hasCoupon = !couponId.isEmpty() && !couponId.equalsIgnoreCase("NULL");
            boolean hasConsumed = !dateUsed.isEmpty() && !dateUsed.equalsIgnoreCase("NULL");
    
            // 行为类型标记
            if (hasCoupon) {
                if (hasConsumed) {
                    type.set("positive");
                } else {
                    type.set("negative");
                }
            } else if (hasConsumed) {
                type.set("normal");
            } else {
                return; // 无券且无消费，过滤
            }
    
            merchantId.set(mId);
            context.write(merchantId, type);
        }
    }
    
    public static class IntSumReducer 
        extends Reducer<Text, Text, Text, Text> {
        
        private Text result = new Text();
        
        public void reduce(Text key, Iterable<Text> values, 
                          Context context
                          ) throws IOException, InterruptedException {
            
            int negativeCount = 0;
            int normalCount = 0;
            int positiveCount = 0;
            
            for (Text val : values) {
                String type = val.toString();
                switch (type) {
                    case "negative":
                        negativeCount++;
                        break;
                    case "normal":
                        normalCount++;
                        break;
                    case "positive":
                        positiveCount++;
                        break;
                }
            }
            
            result.set(negativeCount + "\t" + normalCount + "\t" + positiveCount);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "merchant analysis");
        job.setJarByClass(Task1_MerchantAnalysis.class);
        job.setMapperClass(TokenizerMapper.class);
        // 注意：不使用Combiner，避免类型不匹配问题
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
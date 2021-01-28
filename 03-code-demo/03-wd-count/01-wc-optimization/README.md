# WordCount案例代码优化一

接[上节](../README.md)。针对硬编码问题，优化代码如下：

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WcDemo2 extends Configured implements Tool {

    //Map
    public static class MrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text mapOutputKey = new Text();
        private IntWritable mapOutputValue = new IntWritable(1);

        @Override
        //每读取一行，都会调用一次map()
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split(" ");
            for (String str : strs) {
                mapOutputKey.set(str);
                context.write(mapOutputKey, mapOutputValue);
            }
        }
    }

    //Reduce
    public static class MrReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputV = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            outputV.set(sum);
            context.write(key, outputV);
        }
    }

    //Driver
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建Job
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        //设置Job
        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MrMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MrReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //提交Job
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
//        args = new String[] {
//                "hdfs://n1.com.rwj:8020/Test/input",
//                "hdfs://n1.com.rwj:8020/Test/wcDemo2"
//        };
//        int status = new WcDemo2().run(args);
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new WcDemo2(), args);
        System.exit(status);
    }
}
```


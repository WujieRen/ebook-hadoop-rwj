# WordCount案例代码优化二

针对WordCount案例代码中提出的第二个可优化点，这里通过指定Map端的Combine来解决。

## Combine是做什么的？

Combine类似于一个Reduce，只不过是在Map端执行的一个小的Reduce。

用于减少在Map -> Reduce中间传输的数据量。尽管Combine是可选的，但它有助于任务更有效的执行。

## Combine是如何工作的？

Combine没有预定义的接口，但是它必需实现Reducer接口的reduce方法。

Combine会对Map端的每个输出结果进行操作，它必须有和Reducer类相同的输出：<key,value>类型。

Combine可以从数据集上生成摘要信息，因为它代替了原始的Map输出。

## 代码案例

实现方式：自定义一个Combine类，实现Reducer接口并实现其reduce方法。具体代码如下：

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CombineDemo {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text mapOutputKey = new Text();
        private IntWritable moV = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strs = line.split(" ");
            for (String str : strs) {
                mapOutputKey.set(str);
                context.write(mapOutputKey, moV);

                System.out.println(str + "  @@@  " + moV.get());
            }
        }
    }

    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable oV = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            oV.set(sum);
            context.write(key, oV);

            System.out.println(key + "  =!!!!=  " + oV.get());
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable oV = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            oV.set(sum);
            context.write(key, oV);

            System.out.println(key + "  ====  " + oV.get());
        }
    }

    private int run(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //shuffle
        job.setCombinerClass(Combiner.class);

        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        args = new String[] {
//                "hdfs://n1.com.rwj:8020/Test/input",
//                "hdfs://n1.com.rwj:8020/Test/ComBT2"
            
                // 本地测试可以用本地路径，方便查看结果
                "data/wc/wc.txt",
                "data/wc/output1"
        };
        int status = new CombineDemo().run(args);
        System.exit(status);
    }
}
```


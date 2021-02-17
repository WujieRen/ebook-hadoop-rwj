# MapReduce编程模板

## MapReduce原理

MapReduce的过程可分为5个阶段：input --> map() --> shuffle --> reduce()  --> output。

- input：数据输入。
- output：结果输出。

以下重点说以下map()、reduce()、和shuflle阶段。

### map()

#### Map起始阶段

Map起始阶段，首先会按照*job.setInputFormatClass()*定义的输入格式（InputFormat)，将输入的数据集分割(*getSplits()*)成小数据块。

同时InputFormat提供一个RecordReader的实现。一般读入文本使用的是TextInputFormat，它提供的RecordReader会将文本的行号作为Key，这一行的文本作为Value。这就是指定Mapper的输入为<LongWritable,Text>的原因。

然后调用自定义Mapper的map方法，将一个个<LongWritable,Text>键值对输入给Mapper的map方法。自定义Mapper中的map方法就是我们对数据的操作逻辑。

#### Map最后阶段

map方法执行完毕后，会先调用*job.setPartitionerClass()*对这个Mapper的输出结果进行分区，每个分区映射到一个Reducer。每个分区内又会调用*job.setSortComparatorClass()*设置的类对Key进行排序。如果没有通过*job.setSortComparatorClass()*设置Key比较类，则使用Key类实现的compareTo方法。

## reduce()

在Reduce阶段，reduce()方法接收到所有映射到该Reduce的map的输出结果后，也会调用*job.setSortComparatorClass()*方法设置的Key比较器类，对所有数据进行排序。

然后开始构造一个Key对应的Value迭代器。这时就要用到分组，使用*job.setGroupingComparatorClass*方法可以设置分组类。只要分组类的Grouping规则比较两个Key是相同的，它们就属于同一组，它们的Value 就放在一个同一个迭代器中。而这个迭代器的Key使用属于同一个组的所有的Key的第一个。

最后进入到Reducer的reduce方法，reduce方法输入的是所有的Key及其对应的迭代器。注意输入与输出的类型必须与自定义的Reducer中声明的一致。



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

/**
 * Hello world!
 */
public class MRModule extends Configured implements Tool {

    
    public static class MrMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    //Reduce
    public static class MrReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        }
    }

    //Driver
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建Job
//        Configuration conf = new Configuration();
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        //设置Job
        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);

        //设置Mapper类  ---  Map阶段
        job.setMapperClass(MrMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        //TODO:             shuffle阶段
        //分区
        //job.setPartitionerClass(CusPartitioner.class);
        //排序
        //job.setSortComparatorClass();
        //优化
        //job.setCombinerClass();
        //分组
        //job.setGroupingComparatorClass(CusGrouping.class);
        
        //设置Reudcer类   ---   Reduce阶段
        job.setReducerClass(MrReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //提交Job
        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
//        int status = new MRModule().run(args);
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf, new MRModule(), args);
        System.exit(status);
    }
}
```


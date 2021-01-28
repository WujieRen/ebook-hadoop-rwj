# WordCount案例

WordCount的流程是这样的：针对输入的文件，按照指定的分隔符将字段分割，并形成<key, value>形式的键值对。Map部分每读取一行数据都会执行一次，然后将分组后的结果交给Reduce处理。

比如输入文件是这样的：

```txt
hadoop hive hive 
scala spark hadoop 
```

那MapReduce程序Map端实现：将文字分割，然后形成<word, 1>格式的<key,value>对。，都会调用一次。以上文件分割后形成：

```txt
<hadoop,1>
<hive,1>
<hive,1>
<scala,1>
<spark,1>
<hadoop,1>
```

然后Reduce端接收到的结果是按照上述<key,value>的key进行分组后的结果，这里为什么Reduce端拿到的是上述结果分组后的结果呢？简单来说就是默认情况下，MapReduce已经指定了Map -> Reduce中间的分组和排序的规则。这个规则我们是可以自定义的，可以参考：[二次排序](../04-second-sort/README.md)。

最后Reducer会按照Reduce中指定具体的reduce规则，计算出每个单词出现的个数。即：

```txt
<hadoop,[1,1]>   ===>   <hadoop,2>
<hive,[1,1]>     ===>   <hive,2>
<scala,1>        ===>   <scala,1>
<spark,1>        ===>   <spark,1>
```

以上流程具体实现代码：

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

public class WcDemo1 {

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
    private int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建Job
        Configuration conf = new Configuration();
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

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        args = new String[] {
                "hdfs://n1:8020/Test/input",
                "hdfs://n1:8020/Test/wcDemo1"
        };
        int status = new WcDemo1().run(args);

        System.out.println(status+"--------------------");

        System.exit(status);
    }
}
```

直接在Windows本地运行以上代码，就可以在HDFS Web UI界面看到运行后生成的结果。但是还有两个可以优化的地方。

首先，如果要将Java文件打包为jar包传到服务器上运行，遮掩过的方式显然不太合适。因为输入文件和输出文件的路径给死了，属于硬编码。优化代码见：

- [优化一](./01-wc-optimization/README.md)

另外，以上只是以两行很少的单词作为输入文件举例。如果输入文件过大的话，在Map -> Reduce过程中的shuffle过程传输的数据量也会非常大，这种情况下就会占用大量的网络带宽。这时候我们可以通过用Map端的Combine（合并）来让Map端提前做一点合并，承担一点Reduce端的工作。优化方式见：

- [自定义Map端Combine规则](./02-wc-custom-combine/README.md)


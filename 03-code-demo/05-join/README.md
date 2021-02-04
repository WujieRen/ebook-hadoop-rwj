# MapReduce实现SQL的Join功能

## 需求

案例需求是这个样子的：现有两个文件，一个文件是customer.csv——存储的是客户信息，另一个文件是order.csv——存储的是订单信息。内容分别如下（#后面的是注释内容）：

customer.csv

```txt
# id	name	phone
1,Stephanie Leung,555-555-5555
2,Edward Kim,123-456-7890
3,Jose Madriz,281-330-8004
4,David Stork,408-555-0000
```

orders.csv

```txt
# id	goods	price	date
3,Apple,12.95,02-Jun-2008
1,Banana,88.25,20-May-2008
2,Patato,32,30-Nov-2007
3,Orange,25.02,22-Jan-2009
```

请用MapReduce实现SQL的Join功能，最终形成以下结果：

```txt
# id	name	phone	goods	price	date
1,Stephanie Leung,555-555-5555,88.25,20-May-2008
2,Edward Kim,123-456-7890,32.00,30-Nov-2007
3,Jose Madriz,281-330-8004,25.02,22-Jan-2009
3,Jose Madriz,281-330-8004,12.95,02-Jun-2008
```

## 实现

具体思路：在Map端，将customer和order每一行的数据分割为<id,data>的形式，这样，在数据从Map端流转到Reduce的时候，就形成了<id,[customer1,order1,order2...]>的形式。然后在Reduce端，将信息打平，形成多条customer和order的组合信息：<id,customer1+order1>、<id,customer1+order2>，即可。

这里需要我们自定义一个数据类型，来区分解析后得数据是客户（customer）信息，还是订单（order）信息。自定义数据类型需要实现Writable接口。

```java
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class DataJoinWritable implements Writable {

    private String tag;
    private String data;

    public DataJoinWritable() { }

    private DataJoinWritable(String tag, String data) {
        this.set(tag, data);
    }

    public void set(String tag, String data) {
        this.setTag(tag);
        this.setData(data);
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.getTag());
        out.writeUTF(this.getData());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setTag(in.readUTF());
        this.setData(in.readUTF());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataJoinWritable that = (DataJoinWritable) o;
        return Objects.equals(tag, that.tag) &&
                Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, data);
    }

    @Override
    public String toString() {
        return tag + "," + data;
    }
}
```

然后实现MapRedue任务的代码：

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataJoinMapReduce extends Configured implements Tool {


    public static class DataJoinMapper extends Mapper<LongWritable, Text, LongWritable, DataJoinWritable> {

        private LongWritable mKey = new LongWritable();
        private DataJoinWritable mVal = new DataJoinWritable();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            if(key.get() == 0) {
                return;
            }
            String[] values = line.toString().split(",");

            if(3 != values.length && 4 != values.length) { return; }

            long cid = Long.parseLong(values[0]);

            if(3 == values.length) {
                String name = values[1];
                String phone = values[2];
                mKey.set(cid);
                mVal.set("customer", name + "," + phone);
//                context.write(mKey, mVal);
            }

            if(4 == values.length) {
                String goods = values[1];
                String price = values[2];
                String date = values[3];
                mKey.set(cid);
                mVal.set("order", goods + "," + price + "," + date);
//                context.write(mKey, mVal);
            }

            System.out.println(mKey+":"+mVal);

            context.write(mKey, mVal);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }


    public static class DataJoinReducer extends Reducer<LongWritable, DataJoinWritable, NullWritable, Text> {
        private Text rVal = new Text();

        @Override
        public void reduce(LongWritable key, Iterable<DataJoinWritable> values, Context context) throws IOException, InterruptedException {

            String customerInfo = "";
            List<String> orderList = new ArrayList<>();
            for(DataJoinWritable val : values) {
                if(val.getTag().equals("customer")) {
                    customerInfo = val.getData();
                }
                if(val.getTag().equals("order")) {
                    orderList.add(val.getData());
                }
            }

            for(String orderInfo : orderList) {
                rVal.set(key.get() + "," + customerInfo + "," + orderInfo);
                context.write(NullWritable.get(), rVal);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(DataJoinMapReduce.class);

        Path inputPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inputPath);
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(DataJoinMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(DataJoinWritable.class);

        job.setReducerClass(DataJoinReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new String[] {
//                "hdfs://n3.com.rwj:8020/test/join/input" , "hdfs://n3.com.rwj:8020/test/join/output1"
                "data/join/input",
                "data/join/output"
        };
        int isSuccess = ToolRunner.run(conf, new DataJoinMapReduce(), args);
        System.exit(isSuccess);
    }
}
```


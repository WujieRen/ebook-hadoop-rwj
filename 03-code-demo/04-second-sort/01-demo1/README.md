# 二次排序案例一：

需求：以下数据，实现先按照第一列（字母）排序，然后，第一列顺序相同的，按照第二列（数字）排序。

```txt
a,1
a,3
a,100
b,3
b,2
z,10
b,1
c,5
c,3
c,1
```

## 思路

MapReduce过程中，在map方法执行完毕后，会先调用*job.setPartitionerClass()*对这个Mapper的输出结果进行分区，每个分区映射到一个Reducer。每个分区内又会调用*job.setSortComparatorClass()*设置的类对Key进行排序。如果没有通过*job.setSortComparatorClass()*设置Key比较类，则使用Key类实现的compareTo方法。

首先一个解决方案就是，自定义一个数据类型来作为Key类，这个数据类型就是第一列和第二列的组合，如：(a,1)。Valu的值还是保持第二列的数字。这样只需要对Key排序，就顺带着把Value的值也排序了。

这个数据类型的compareTo()方法实现的逻辑就是：先按照第一个值进行比较，如果第一个值相等，就按照第二个值进行比较。这样，MapReduce过程中的排序规则默认就是我们自定义数据类型的比较规则。



## 代码

### 自定义Key类

MapReduce中使用自定义数据类型作为Key，必须实现WritableComparable接口。

```java
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * MapReduce中使用自定义数据类型作为Key，必须实现WritableComparable接口。
 *
 * WritableComparables can be compared to each other, typically via Comparators.
 * Any type which is to be used as a key in the Hadoop Map-Reduce framework should implement this interface.
 */
public class CusWritable implements WritableComparable<CusWritable> {

    private String first;
    private int second;

    public CusWritable() { }

    public CusWritable(String first, int second) {
        this.set(first, second);
    }

    public void set(String first, int second) {
        this.setFirst(first);
        this.setSecond(second);
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CusWritable that = (CusWritable) o;
        return second == that.second &&
                StringUtils.equals(first, that.first);
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
    
    @Override
    public int compareTo(CusWritable o) {
        int fr = this.getFirst().compareTo(o.getFirst());
//        return fr;
        if(0 != fr) { return fr; }
        //TODO: 核心就在这里，如果没有下面这个语句块，那默认就是按照字典序排列的，100就一定会排在3前面
        return Integer.compare(this.getSecond(), o.getSecond());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        //TODO:这里必须配套使用，如果 写 和 读  的类型不一样会报错： EOFException
//        out.write(second);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}
```



### MapReduce任务

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


public class SecondarySortDemo {
    public static class SSMap extends Mapper<LongWritable, Text, CusWritable, IntWritable> {
        private  CusWritable moKey = new CusWritable();
        private IntWritable moVal = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split(",");
            if (2 != strs.length) {
                return;
            }

            moKey.set(strs[0], Integer.parseInt(strs[1]));
//            moKey.set(strs[0], Integer.valueOf(strs[1]));
            moVal.set(Integer.valueOf(strs[1]));
            context.write(moKey, moVal);
        }
    }

    public static class SSReduce extends Reducer<CusWritable, IntWritable, Text, IntWritable> {
        private Text outKey = new Text();

        @Override
        public void reduce(CusWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//            outKey.set(key.getFirst());
            System.out.println(key.toString()+"-----------");
            for(IntWritable val : values) {
                outKey.set(key.getFirst());
                context.write(outKey, val);
            }
        }
    }

    private int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setMapperClass(SSMap.class);
        job.setMapOutputKeyClass(CusWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        //TODO:shuffle阶段
        //分区
//        job.setPartitionerClass(CusPartitioner.class);
//        job.setPartitionerClass(UpperLowerPartitioner.class);
        //排序
//        job.setSortComparatorClass();
        //优化
//        job.setCombinerClass();
        //分组
//        job.setGroupingComparatorClass(CusGrouping.class);

        job.setReducerClass(SSReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //Reduce个数  默认就是1，有几个reduce最后就会生成几个输出文件
//        job.setNumReduceTasks(1);

        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        args = new String[] {
                "data/secondarysort/secondarysort.txt",
                "data/secondarysort/output5"
        };
        int status = new SecondarySortDemo().run(args);
        System.exit(status);
    }
}
```

## 优化

其实以上代码足以解决需求，但其实有几个隐含的地方并没有处理好。它已经实现了自定义数据类型，以及间接设置了job.setSortComparatorClass()——不设置默认按照Key类的compareTo()方法。但是还是有几个很明显的缺点：即分区和分组的规则。如果不设置，默认也按照Key相等的进行分区和分组。即按照自定义数据类型去分区和分区了。

即分区和分组的key并不是a、b、c，而是按照(a,1)、(b,1)、(c,1)来排序的。以上操作在map结束后，形成的<Key,Value>键值对是：<(a,1), 1>、<(a,3), 3>、<(a,100), 100>。这样的确对Key排序后，Value也就有序了。

但是当数据量很大且某些字母出现次数占比较大时，这样的分区和分组效果就不好了，不严谨而且很容易造成数据倾斜。这时候，如果我们希望分区和分组规则更严谨一点，就需要我们自定义分区器和比较器了。

### 自定义分区类

这里有一个常用的场景就是通过以下方法来解决数据倾斜。比如数据集某些值占比过多，导致处理该值的reduce久久处理不完，导致任务效率极低无法结束，就可以通过该方法解决。通过哈希取余来将数据分到指定个reduce上。

```java
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器
 *      Partitioner<KeyTye, ValueType>
 */
public class CusPartitioner extends Partitioner<CusWritable, IntWritable> {

    @Override
    //key.hashCode() & 2147483647  可以保证结果是非负数
    public int getPartition(CusWritable o, IntWritable o2, int numPartitions) {
//        return (o.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        return (o.getFirst().hashCode() & 2147483647) % numPartitions;
    }
}
```



### 自定义分组类

```java
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组规则
 */
public class CusGrouping implements RawComparator<CusWritable> {

    /**
     * Compare two objects in binary.
     * b1[s1:l1] is the first object, and b2[s2:l2] is the second object.
     *
     * @param b1 The first byte array.
     * @param s1 The position index in b1. The object under comparison's starting index.
     * @param l1 The length of the object in b1.
     * @param b2 The second byte array.
     * @param s2 The position index in b2. The object under comparison's starting index.
     * @param l2 The length of the object under comparison in b2.
     * @return An integer result of the comparison.
     */
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        // 一个int是4个字节，所以这里是 -4
        return WritableComparator.compareBytes(b1, 0, l1 - 4, b2, 0, l2 - 4);
    }

    @Override
    public int compare(CusWritable o1, CusWritable o2) {
        return o1.getFirst().compareTo(o2.getFirst());
    }
}
```


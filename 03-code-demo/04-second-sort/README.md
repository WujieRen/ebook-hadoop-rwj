# 二次排序

默认情况下，在MapReduce程序中，Map输出的结果<Key,Value>是按照默认的排序规则（字典序排序），只对对Key进行排序。因此Value的排序经常是不固定的。但是我们经常会遇到对Key和Value都需要排序的需求；比如Hadoop权威指南中求一年的最高气温，Key为年份，Value为最高气温；还有电商网站经常有按照天统计商品销售排行等需求。这些需求需要对Key和Value都进行排序，这时候就需要用到二次排序了。

## 二次排序原理

二次排序的核心是要对MapReduce的整个流程有很细致的了解。可以把二次排序分为以下几个阶段：

### Map起始阶段

在Map阶段，使用*job.setInputFormatClass()*定义的InputFormat，将输入的数据集分割(*getSplits()*)成小数据块，同时InputFormat提供一个RecordReader的实现。在这里使用的是TextInputFormat，它提供的RecordReader会将文本的行号作为Key，这一行的文本作为Value。这就是指定Mapper的输入为<LongWritable,Text>的原因。然后调用自定义Mapper的map方法，将一个个<LongWritable,Text>键值对输入给Mapper的map方法。

### Map最后阶段

在Map阶段的最后，会先调用*job.setPartitionerClass()*对这个Mapper的输出结果进行分区，每个分区映射到一个Reducer。每个分区内又会调用*job.setSortComparatorClass()*设置的类对Key进行排序。如果没有通过*job.setSortComparatorClass()*设置Key比较类，则使用Key类实现的compareTo方法。

### Reduce阶段

在Reduce阶段，reduce()方法接收到所有映射到该Reduce的map的输出结果后，也会调用*job.setSortComparatorClass()*方法设置的Key比较器类C，对所有数据进行排序。然后开始构造一个Key对应的Value迭代器。这时就要用到分组，使用*job.setGroupingComparatorClass*方法设置分组类。只要分组类的Grouping规则比较两个Key是相同的，它们就属于同一组，它们的Value 就放在一个同一个迭代器中。而这个迭代器的Key使用属于同一个组的所有的Key的第一个。

最后进入到Reducer的reduce方法，reduce方法输入的是所有的Key及其对应的迭代器。注意输入与输出的类型必须与自定义的Reducer中声明的一致。


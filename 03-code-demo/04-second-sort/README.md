# 二次排序

默认情况下，在MapReduce程序中，Map输出的结果<Key,Value>是按照默认的排序规则（字典序排序），只对对Key进行排序。因此Value的排序经常是不固定的。但是我们经常会遇到对Key和Value都需要排序的需求；比如Hadoop权威指南中求一年的最高气温，Key为年份，Value为最高气温；还有电商网站经常有按照天统计商品销售排行等需求。这些需求需要对Key和Value都进行排序，这时候就需要用到二次排序了。

## 二次排序原理

理解二次排序的前提是要对MapReduce的整个流程有很细致的了解。二次排序的核心就是合理利用数据在MapReduce过程中的分区（job.setPartitionerClass）、排序（job.setSortComparatorClass）、分组（ob.setGroupingComparatorClass）规则，以及自定义数据类型的compareTo()方法，来实现让数据按照我们指定的排序规则进行排序。

下面直接用几个案例来体会。




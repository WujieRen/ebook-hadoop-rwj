# 集群安装

## 简介

​	描述如何搭建一个完全分布式的Hadoop集群，以3台服务器举例。

## 机器规划

| n1                          | n2            | n3                |
| :-------------------------- | :------------ | :---------------- |
| NameNode/DataNode           | DataNode      | DataNode          |
| ResourceManager/NodeManager | NodeManager   | NodeManager       |
|                             | HistoryServer | SecondaryNameNode |
|                             |               |                   |


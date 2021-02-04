# Windows本地开发Hadoop代码环境配置

1. 安装好jdk、maven、idea等基础组件，具备Windows本地开发Java代码的环境；

2. [下载Hadoop](https://archive.apache.org/dist/hadoop/common/)，并解压到本地；

3. [下载](https://github.com/cdarlint/winutils)相应版本的winutils.exe文件和hadoop.dll文件，放到Windows本地的$HADOOP_HOME/bin目录下；

4. 如果要把数据上传到HDFS上，在Windows本地和HDFS远程交互的话，需要在resource目录下添加我们安装好的集群中的：core-site.xml和hdfs-site.xml，两个配置文件；如果数据只是在本地，全程开发和测试都只是在本地，不和HDFS进行数据交互的话，就不需要这两个文件，否则会报异常（找不到路径）；

5. 创建Maven项目，在pom.xml中添加开发Hadoop代码所需依赖包。

   ```xml
   <dependency>
        <groupId>org.apache.hadoop</groupId>
        <artifactId>hadoop-client</artifactId>
        <version>${hadoop.version}</version>
   </dependency>
   ```

配置好以后的目录可参考：
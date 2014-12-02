stats-hdfs
==========
# 1.概述
==========
该项目是基于hadoop-2.5.1，编译了hadoop提供的example，并修改了example中的部分代码（更具自己的需求改写）；同时也添加了自己研究的一部分源码。

# 2.项目结构
stats-hdfs的目录结构图如下：
https://github.com/dengjie420/maven-repository/blob/master/image/hadoop/example/QQ20141202-1%402x.png
`main`目录下存放单独mr任务的启动程序入口
`mapreducer`目录存储的是自定义的map和reduce类
`example`目录存放的是hadoop-2.5.1源码自带的例子和自己专研的例子
`utils`目录存放的是封装中的java工具类

# 3.项目依赖
stats-hdfs项目采用maven项目结构，需要依赖项目仓库，为了共享仓库，我将仓库建在github上方便其他项目使用。
### 仓库地址如下：
https://github.com/dengjie420/maven-repository
===========
<h3>热爱生活，享受编程</h3>
邮箱：smartdengjie@gmail.com

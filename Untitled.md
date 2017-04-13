#dolphin 使用说明

* 程序jar：dolphin_2.10-0.0.1.jar
* Main方法入口：com.maogogo.dolphin.Main
* 使用spark-submit执行 spark-submit --class com.maogogo.dolphin.Main dolphin_2.10-0.0.1.jar, 其他参数可适当添加
* 定时流程最好使用shell循环调用spark-submit命令

###依赖

```
"org.apache.spark" %% "spark-core" % "1.6.1",
"org.apache.spark" %% "spark-sql" % "1.6.1",
"org.apache.spark" %% "spark-hive" % "1.6.1",
"com.databricks" %% "spark-csv" % "1.5.0",
"mysql" % "mysql-connector-java" % "5.1.39",
"org.apache.commons" % "commons-csv" % "1.4",
"com.typesafe" % "config" % "1.3.1",
"log4j" % "log4j" % "1.2.17",
"org.freemarker" % "freemarker" % "2.3.23"
```


#配置文件

程序启动需要指定配置文件，以conf为扩展名

> input: 必须，指定需要执行的xml<br>
> tpl_path: 必须，指定存储替换好的xml路径<br>
> params: Map 结构，指定需要替换的参数，使用这个参数自动替换xml文件中出现的${key}


```json
input="demo2.xml"
tpl_path=tpl
params = {
	date="112233"
	name="aabbcc"
}
```


#配置文件说明

##format
下一个版本支持，现在程序里面写好的，主要是对CSV数据进行转换的参数<br>
现在csv文件导入和导出都是以“|”分割
##dbsource
配置数据库源，提供程序连接数据库使用，上级目录dbsources

> name: 自定名称，便于在transform中使用<br>
> url: 数据库连接<br>
> username: 用户名<br>
> password: 密码<br>

```xml
<source name="oracle" url="jdbc:oracle:thin:@10.182.52.9:1521/chnldb" username="channel" password="channel" />
```

##transform
上级目录transforms

> from: 需要转换的类型，支持类型：csv、parquet、orc、sql(已经注册为零时表才能使用这个配置), hadoop(新增)<br>
> fromPath: 配合from使用，指定路径<br>
> to: 转换为数据类型，支持：csv、parquet、orc、空(不做任何处理)<br>
> toPath：配置to使用，指定路径<br>
> table：暂不使用<br>
> sql：SQL语句会sql文件路径，从from数据执行的sql语句<br>
> tmpTable: 对from的数据注册临时表<br>
> process：sql语句或sql文件路径，已经注册好的临时表，可以使用这个参数，对载入的数据进行下一步处理<br>
> hiveTable: 暂不使用<br>
> model： 默认override, 支持override、error、append<br>
> cache: 暂不使用<br>
> action: from 类型为 hadoop是有效， remove、move、merge、getmerge
> local:  from 类型为 hadoop是有效， 输出到本地路径


```xml
<transform from="csv" to="" fromPath="/data/in/toan/${date}/bb.txt"
			toPath="" format="" table="" sql="" tmpTable="p_t_component_d"
			hiveTable="" mode="" process="select a from p_t_component_d group by a"
			cache="true"/>
```

##column
上级目录columns, 必须放在transform中使用，主要是配合导入csv文件使用

> name: 列名<br>
> cname: 映射列名, 暂不使用<br>
> type: 类型, 默认都给String类型，暂不支持配置<br>
> nullable：true， 暂不支持配置<br>
> format: 日期格式化，暂不支持配置<br>

```xml
<columns>
	<column name="a" cname="" type="" nullable="" />
	<column name="b" cname="" type="" nullable="" />
	<column name="c" cname="" type="" nullable="" />
	<column name="d" cname="" type="" nullable="" format="date" />
</columns>
```

##subtransform

* 上级目录transform，和transform参数一致
* 子流程处理，会自动带入transform的处理结果集，自动循环处理
* transform中的数据会自动做成List[Map]，在subtransform中可使用表达式{key}获取数据，现在主要是toPath和sql参数有效

#待开发
* 完善日志功能
* 加入日期转换功能
* 字段映射功能
* 测试spark相关参数
* csv文件格式化功能

#开发日志
###20170410
* 添加log4j日志
* 添加conf文件作为输入参数配置

###20170402
* 加入subform处理流程
* 添加process处理流程







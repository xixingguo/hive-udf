# hive-udf
hive udf 自定义json数组解析
要适应自己的平台，得修改对应的jar版本


使用步骤：
1、打jar
2、将jar上传到linux服务器或hdfs上
3、在hive-client使用：
	创建临时函数：
	hive>add jar /home/hadoop/json_array.jar; --jar包在服务器的路径，也可以是hdfs的路径
	hive>create temporary function json_array as ‘com.hxg.udf.Json_array’;

	创建永久函数：
	hive>create function json_array as 'com.hxg.udf.Json_array' using jar '/home/hadoop/json_array.jar';

此函数配合 lateral view explode(json_array(此处为hive表的json数组字段)) 使用
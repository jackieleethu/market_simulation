scala_library(
  name = 'scala_lib',
  srcs = glob(['*.scala','*/*.scala','*/*/*.scala'], excludes = []),
  deps = [
   #'//app/qzap/proto/log:log',
   #'//app/qzap/proto/db:ad_clone_source_proto',
   '//app/qzap/service/targeting/ucube/common/log:log_parser',
   '//app/qzap/service/targeting/ucube/common/scala:hadoop_spark_log',
   '//app/qzap/service/targeting/ucube/common/scala:scala_common',
   #'//exp/exp_param_parser:exp_param_parser',
   #'//exp/exp_param_parser/proto:exp_param_proto',
   #'//exp/exp_param_parser/proto:topology_proto',
   '//java/com/gdt/log_process/log_format:log_format',
   '//java/com/gdt/log_process/ocpa/common/dstream:dstream',
   #'//java/com/gdt/log_process/ad_clone/utils:mysql_util',
   '//java/com/gdt/recordio/mapreduce:recordio_mapreduce',
   '//java/exp/market_simulation/proto:market_simulation_proto',
   '//java/exp/market_simulation/proto:market_simulation_config_proto',
  ],
  provided_deps = [
    '//thirdparty/java/deps:kafka',
    '//thirdparty/java/deps:guava-14.0.1',
    '//thirdparty/java/deps:hadoop-common',
    '//thirdparty/java/deps:spark-core',
    '//thirdparty/java/deps:spark-toolkit_2.11',
    '//thirdparty/java/deps:spark-sql',
    '//thirdparty/java/deps:mysql_connector',
    '//thirdparty/java/deps:spark-streaming',
    '//thirdparty/java/deps:tdw-spark-mllib',
  ],
)

java_fat_library(
  name = 'market_simulation_streaming',
  deps = [
    ':scala_lib'
  ],
  exclusions = [
    'org.apache.hadoop:*:*',
    'org.apache.spark:*:*',
    'com.typesafe.akka:*:*',
    'org.scala-lang:scala-library:*',
    'javax.servlet:servlet-api:*',
    'javax.ws.rs:*:*',
    'com.sun.jersey:*:*',
    'com.esotericsoftware.kryo:*:*',
    'org.scala-lang:scala-reflect:*',
    'com.google.protobuf:*:*',
    'io.netty:*:*',
    'net.jpountz.lz4:*:*',
    'org.codehaus.janino:*:*',
    'com.alibaba:fastjson:*',
  ]
)

java_fat_library(
  name = 'market_simulation_streaming_deps',
  deps = [
   '//app/qzap/proto/log:log',
   '//app/qzap/proto/db:ad_clone_source_proto',
   '//app/qzap/service/targeting/ucube/common/log:log_parser',
   '//app/qzap/service/targeting/ucube/common/scala:hadoop_spark_log',
   '//app/qzap/service/targeting/ucube/common/scala:scala_common',
   '//exp/exp_param_parser:exp_param_parser',
   '//exp/exp_param_parser/proto:exp_param_proto',
   '//exp/exp_param_parser/proto:topology_proto',
   '//java/com/gdt/log_process/log_format:log_format',
   '//java/com/gdt/log_process/ocpa/common/dstream:dstream',
   '//java/com/gdt/log_process/ad_clone/utils:mysql_util',
   '//java/com/gdt/recordio/mapreduce:recordio_mapreduce',
   '//java/exp/market_simulation/proto:market_simulation_proto',
   '//java/exp/market_simulation/proto:market_simulation_config_proto',
   '//thirdparty/java/deps:kafka',
   '//thirdparty/java/deps:guava-14.0.1',
   '//thirdparty/java/deps:hadoop-common',
   '//thirdparty/java/deps:spark-core',
   '//thirdparty/java/deps:spark-toolkit_2.11',
   '//thirdparty/java/deps:spark-sql',
   '//thirdparty/java/deps:mysql_connector',
   '//thirdparty/java/deps:spark-streaming',
   '//thirdparty/java/deps:tdw-spark-mllib',
  ],
)

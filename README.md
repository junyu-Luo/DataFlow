# DataFlow

- 一个可配置化生成数据集的工具
  - Operator.py ：自定义py函数
  - fc_update.py ： pyspark统计获取vector、统计值等
  - DataProcess.py ： 数据集生成主类

- 生成tfrecord需要在spark添加配置jar包：spark-tfrecord_2.12-0.4.0.jar
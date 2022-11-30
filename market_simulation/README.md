# 双边市场模拟spark任务

## 项目简介
广告投放系统可以视为广告供给侧和用户需求侧构成的一种双边市场。为了量化双边市场中各种要素对市场供需的影响，并探索如何最大化双边市场的收益，我们希望通过模拟的方式来对双边市场进行分析和研究。

## 设计文档
https://doc.weixin.qq.com/doc/w3_ACAAkQaDACc2RuJVh7qQ2iyi5lobz?scode=AJEAIQdfAAocKyCK3QACAAkQaDACc

## 文件说明
### MarketSimulationStreamingMain.scala
双边市场模拟spark streaming任务主入口，按照设计的模拟流程依次启动/调用各模块的模拟方法
streaming任务不会自动结束，任务失败后可基于checkpoint继续跑任务，适合大规模的模拟。
### MarketSimulationMain.scala
双边市场模拟spark任务主入口，按照设计的模拟流程依次启动/调用各模块的模拟方法
spark任务模拟完成后可自动结束，适合小规模的模拟。
### MarketSimulationAnalysisMain.scala
双边市场模拟数据分析spark任务主入口，将模拟中落入HDFS的结构化数据，转为DB数据并入库至TDW，方便分析。
### module/ElementSimulation.scala
对象模拟模块，对用户、广告等对象进行模拟
### module/DisplaySimulation.scala
播放模拟模块，对播放流程进行模拟
### module/EffectSimulation.scala
效果模拟模块，对请求及其播放的广告的效果进行模拟
### module/AdStrategySimulation.scala
离线策略模拟模块，对离线的广告策略，例如调价等策略进行模拟
### module/Utils.scala
通用方法模块

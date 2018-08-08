# stream-drools-example

#### 项目介绍
spark、flink、storm整合drools的例子

#### 软件架构
规则包的编辑在drools-workbench，所以需要先安装drools-workbench

本实例可以使用Jmeter做性能测试，已编写测试类，使用Jmeter调用

#### 安装教程

1. drools-workbench可以百度一下安装
2. 规则需要的Bean一般采用FactType方式，如果使用实体Bean，请将项目中的beans所属包名与wb中一致

#### 使用说明

1. spark本地调试VM-Arguments需要添加-Dspark.master=local[2]
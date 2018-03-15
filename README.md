ease-ali-kafka
===============

基于 node-rdkafka 的阿里云MQ(Kafka) Node.js 客户端. 主要是简化配置和使用方式.    

```bash
npm install ease-ali-kafka
```


## 使用方法
-------------

#### 引入模块

```javascript
const Kafka = require('ease-ali-kafka');
```

#### 初始化

```javascript
Kafka.init({
  bootstrap: 'kafka-ons-internet.aliyun.com:8080',
  username: '你的阿里云帐号 access key',
  password: '你的阿里云帐号 secret 后 10 位',
  consumerID: '在阿里云上配置的 consumer ID', // 如果不需要 consumer 则不用配置该参数
});
```

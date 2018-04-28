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
  // 参考阿里云的接入点说明. https://help.aliyun.com/document_detail/52376.html
  bootstrap: 'kafka-ons-internet.aliyun.com:8080',  
  username: '你的阿里云帐号 access key',
  password: '你的阿里云帐号 secret 后 10 位',
  // 如果不需要 consumer 则不用配置该参数
  consumerID: '在阿里云上配置的 consumer ID',
  // 发送消息后, 等待回执的超时时间, 默认 3 秒
  reportTimeout: 3000,
  // 自动生成 key 的编码方式, 默认 base64, 支持 base64, hex 等
  keyEncode: 'base64',
  // 生产者轮询本地的时间, 默认 200ms
  producerPollInterval: 200,
  // 发送发生超时后的重试次数, 默认重试 2 次
  retries: 2,
});
```

#### 发送消息

```javascript
Kafka.sendKafkaMessage('TOPIC_NAME', {
  id: 123,
  message: 'hello'
}).then(report => console.log('send success, report is ', report))
  .catch(err => console.error('send failed. ', err));
```

**`.sendKafkaMessage(topicName, content)`**    

return: `Promise<report>`

`topicName`: {String} 目标topic名称    
`content`: {String|Object|Buffer} 要发送的内容, 可以直接写 Object.    
`report`: {Object} 消息回执. 其中主要包含字段 `key`{Buffer}, `topic`{String}, `partition`{Number}, `offset`{Number}     

如因网络问题, 发送后3秒内未收到回执, 则会报 timeout 错误.

  
#### 订阅并消费消息

```javascript
Kafka.subscribe('TOPIC_NAME', function (data, commit, next) {
  console.log('consuming data', data);
  commit();
  next();
})
```

**`.subscribe(topicName, handler)`**

return: `undefined`

`topicName`: {String} 目标topic名称     
`handler`: {Function|AsyncFunction} 处理消息的函数. 有多种消费方式    

```javascript
Kafka.subscribe(topic, function (data) {
  // 可以获取到 data. 
  // Kafka 的 commit 操作已在内部执行过了.
  // consumer 不会等待该消息处理完毕就将继续处理下一条消息 
});

Kafka.subscribe(topic, function (data, commit) {
  // 可以获取到 data.
  // 可以自己通过调用 commit() 函数控制是否给 kafka 消费完成的通知
  // commit 的同时会结束当前消息的处理, 准备处理下一条消息 
});

Kafka.subscribe(topic, function (data, commit, next) {
  // 可以获取到 data.
  // 可以自己通过调用 commit() 函数控制是否给 kafka 消费完成的通知
  // 可以通过调用 next() 函数来启动下一条消息的处理, 可以自己手动控制消息的并发度 
  // 这种模式通常用于消息处理过程中包含异步IO的时候 
})
```

`data`: {Buffer} 收到的消息内容, Buffer 类型. 通过 `._metadata` 属性可以获取相关元数据   
`commit`: {Function} 执行该函数则给 Kafka 发送消息成功消费的回执   
`next`: {Function} 执行该函数则开始处理下一个消息. 否则即使有新的消息待处理, 也会等待当前消息处理完成才会处理下一个消息

支持订阅多个不同的 topic.


## License

MIT
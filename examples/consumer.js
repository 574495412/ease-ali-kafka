'use strict';

const Kafka = require('../index');
const configs = require('./configs');

// 先初始化 Kafka, 对于 consumer, 需要设置 consumerID 属性
Kafka.init({
  bootstrap: configs.bootstrap,
  username: configs.username,
  password: configs.password,
  consumerID: configs.consumerID
});

Kafka.subscribe(configs.topicName, function (data, commit, next) {
  console.log('consume message', data);
  commit();
  next();
});

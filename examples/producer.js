'use strict';

const Kafka = require('../index');
const configs = require('./configs');

// 先初始化 Kafka
Kafka.init({
  bootstrap: configs.bootstrap,
  username: configs.username,
  password: configs.password
});

Kafka.sendKafkaMessage(configs.topicName, 'you got a random number:' + Math.random())
  .then(report => console.log('producer report:', report))
  .catch(err => console.error('produce failed', err));


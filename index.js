'use strict';

/*
阿里云 MQ Kafka 版的 Node.js SDK helper
有任何问题请联系: Chunlin-Li
*/

const Kafka = require('node-rdkafka');
const crypto  =require('crypto');
const Writable = require('stream').Writable;

let options = null;
let RP_TIMEOUT = 10000; // 发送消息后, 等待回执的超时时间
let producerWindow = {}; // 用于注册 producer 的 report 回调
let consumers = {}; // 记录所有初始化的 consumer 实例

/**
 * 发送消息
 * @param topic - topic 名称
 * @param content - 发送内容
 * @return Promise<Object> - Promise 的方式返回消息回执 report
 */
exports.sendKafkaMessage = function (topic, content) {

  return getProducer()
    .then(producer => {

      if (typeof content === 'object') {
        content = JSON.stringify(content);
      }
      if (!(content instanceof Buffer)) {
        content = Buffer.from(content);
      }
      let key = crypto.createHash('md5').update(content).digest('base64'); // 计算 md5 作为 key

      producer.produce(topic, -1, content, key); // 发送

      return new Promise((resolve, reject) => {
        // 设定超时计时器
        let timeout = setTimeout(() => {
          delete producerWindow[key];
          reject(new Error(`timeout: did not receive report in ${RP_TIMEOUT} ms`));
        }, RP_TIMEOUT); // RP_TIMEOUT 内没有收到回执则超时
        // 注册 report 回调
        producerWindow[key] = function (report) {
          clearTimeout(timeout); // 关闭超时计时器
          resolve(report);
        };
      })
    });
};

exports.subscribe = function(topic, handler) {

  if (consumers[topic]) {
    throw new Error('repeat subscribe the topic: ' + topic);
  }

  let stream = Kafka.KafkaConsumer.createReadStream({
    // 'debug': 'all',
    'api.version.request': 'true',
    'bootstrap.servers': options.bootstrap, // 上线使用需要改为对应的阿里内网接入点
    'security.protocol': 'sasl_ssl',
    'ssl.ca.location': __dirname + '/ca-cert',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': options.username, // 公共帐号的 access key
    'sasl.password': options.password, // 公共帐号的 secret
    'group.id': options.consumerID,
    'fetch.wait.max.ms': 500,
    'enable.auto.commit': false
  }, {}, {
    topics: [topic]
  });

  consumers[topic] = stream;

  stream.on('ready', function () {
    console.log('consumer stream readable');
  });

  stream.pipe(new Writable({
    write: function (chunk, encoding, callback) { // 实现顺序同步处理, 避免被大量消息打爆
      if (handler.length === 1) { // 只传数据本身, 启动异步模式, 并自动完成读取和提交
        setTimeout(handler.bind(null, chunk), 0);
        stream.consumer.commitMessage(chunk);
        callback();
      } else if (handler.length === 2) { // handler 只提供了 data, commit 两参数时, 提交的同时完成读取
        handler(chunk, function () {
          stream.consumer.commitMessage(chunk);
          callback();
        });
      } else { // 参数按照 data, commit, next 处理. 控制权交由用户
        handler(chunk, () => stream.consumer.commitMessage(chunk), callback);
      }
    },
    objectMode: true
  }));

  stream.on('error', function(err) {
    console.error('kafka consumer stream error', err);
    process.exit(1);
  });

  stream.consumer.on('event.error', function(err) {
    if (err.code !== -1) {
      console.log('event.error', err);
    }
  })

};

/**
 * 初始化 kafka 配置信息
 * @param opt.username - 阿里云公共帐号的用户名, 即帐号的 access key
 * @param opt.password - 阿里云公共帐号的密码, 即帐号的 secret 的后10位
 * @param opt.bootstrap - 阿里云 kafka 接入点. 请查看 https://help.aliyun.com/document_detail/52376.html
 * @param opt.consumerID - 阿里云MQ上配置的消费者ID, 对应 Kafka 中的 groupId 的概念. 一个服务应该使用一个唯一的 ConsumerID
 */
exports.init = function (opt) {
  if (options) {
    return console.error('kafka has been initiated');
  }
  options = opt;
};


function getProducer () {
  if (!options) {
    throw new Error('Kafka not init, please call .init() to initiate');
  }

  if (!getProducer.instance) {

    let producerInstance = new Kafka.Producer({
      // 'debug': 'all',
      'api.version.request': 'true',
      'bootstrap.servers': options.bootstrap, // 上线使用需要改为对应的阿里内网接入点
      'dr_cb': true,
      'dr_msg_cb': false,
      'security.protocol': 'sasl_ssl',
      'ssl.ca.location': __dirname + '/ca-cert',
      'sasl.mechanisms': 'PLAIN',
      'sasl.username': options.username, // 公共帐号的 access key
      'sasl.password': options.password, // 公共帐号的 secret
    });
    let ready = new Promise((resolve, reject) => {
      producerInstance.on('ready', function () {
        console.log('kafka producer connect ok', new Date());
        resolve(producerInstance);
      });
      producerInstance.on('error', function (error) {
        console.error('error:', error);
        reject(error);
      });
    });

    producerInstance.on('disconnected', function () {
      //断线自动重连
      console.error('disconnected');
      producerInstance.connect();
    });
    producerInstance.on('event.error', function (err) {
      //AliKafka服务器会主动掐掉空闲连接，如果发现这个异常，则客户端重连(先disconnect再connect)
      if (-1 !== err.code) {
        console.error(new Date(), 'event.error:', err);
      }
    });
    producerInstance.on('delivery-report', function(err, report) {
      //消息发送成功，这里会收到report
      let reportCallback = producerWindow[report.key.toString()];
      reportCallback && reportCallback(report);
    });

    producerInstance.setPollInterval(200);
    producerInstance.connect();
    getProducer.instance = ready;
  }

  return getProducer.instance;
}

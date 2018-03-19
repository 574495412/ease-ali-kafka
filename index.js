'use strict';

/*
阿里云 MQ Kafka 版的 Node.js SDK helper
*/

const Kafka = require('node-rdkafka');
const crypto  =require('crypto');
const Writable = require('stream').Writable;

let options = null;
let producerWindow = {}; // 用于注册 producer 的 report 回调
let consumerInstance;
let subscribedTopics = {};

/**
 * 初始化 kafka 配置信息
 * @param opt.username - 阿里云公共帐号的用户名, 即帐号的 access key
 * @param opt.password - 阿里云公共帐号的密码, 即帐号的 secret 的后10位
 * @param opt.bootstrap - 阿里云 kafka 接入点. 请查看 https://help.aliyun.com/document_detail/52376.html
 * @param opt.consumerID - 阿里云MQ上配置的消费者ID, 对应 Kafka 中的 groupId 的概念. 一个服务应该使用一个唯一的 ConsumerID
 * @param opt.reportTimeout - 发送消息后, 等待回执的超时时间, 默认 10 秒
 * @param opt.keyEncode
 * @param opt.producerPollInterval
 */
exports.init = function (opt) {
  if (options) {
    return console.error('kafka has been initiated');
  }
  options = Object.assign({
    reportTimeout: 10000,
    keyEncode: 'base64',
    producerPollInterval: 200
  }, opt);
};

/**
 * 发送消息
 * @param topic{String} - topic 名称
 * @param content{String|Object|Buffer} - 发送内容
 * @return Promise<Object> - Promise 的方式返回消息回执 report
 */
exports.sendKafkaMessage = function (topic, content) {

  return getProducer()
    .then(producer => {

      if (typeof content === 'object') {
        content = JSON.stringify(content);
      }
      if (!(content instanceof Buffer)) { // 如果不是 buffer, 则将其转为 buffer
        content = Buffer.from(content);
      }
      let key = crypto.createHash('md5').update(content).digest(options.keyEncode); // 计算 md5 作为 key

      producer.produce(topic, -1, content, key); // 发送

      return new Promise((resolve, reject) => {
        // 设定超时计时器
        let timeout = setTimeout(() => {
          delete producerWindow[key];
          reject(new Error(`timeout: did not receive report in ${options.reportTimeout} ms`));
        }, options.reportTimeout); // reportTimeout 内没有收到回执则超时
        // 注册 report 回调
        producerWindow[key] = function (report) {
          clearTimeout(timeout); // 成功, 关闭超时计时器
          resolve(report);
        };
      })
    });
};

/**
 * 订阅topic
 * @param topic{String} - 要订阅的主题
 * @param handler - 消息处理函数 function(data, commit, next) {}
 */
exports.subscribe = function(topic, handler) {

  if (!options) {
    throw new Error('Kafka not init, please call .init() to initiate');
  }

  let consumer = getConsumer([topic]);

  if (subscribedTopics[topic]) {
    throw new Error('Do not repeat subscribe topic: ' + topic);
  }

  subscribedTopics[topic] = handler;

  if (Object.keys(subscribedTopics).length > 1) { // 追加订阅的 topic, 须再执行一遍 subscribe()
    let topics = Object.keys(subscribedTopics);
    consumer.subscribe(topics);
  }
};


/**
 * 获取 producer 实例, 返回 promise
 * @return {Promise<Object>}
 */
exports.getProducer = () => getProducer.instance;

/**
 * 获取 consumer 实例. 直接返回
 * @return {Object} consumer 实例
 */
exports.consumerInstance = () => consumerInstance;

// 参数 topics 只在首次订阅时有效. 因为创建 ReadStream 时, 参数 topics 不能为空.
function getConsumer (topics) {

  if (!consumerInstance) {

    // 初始化 consumer, stream API 方式
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
    }, {}, {topics: topics});

    stream.consumer.on('ready', function () {
      console.log('kafka consumer connect ok');
    });

    stream.consumer.on('subscribed', function (topics) {
      console.log('kafka consumer subscribed topics: ', topics);
    });

    stream.on('error', function (err) {
      console.error('kafka consumer stream error', err);
    });

    stream.consumer.on('disconnected', function () {
      console.error('kafka consumer disconnected');
    });

    stream.consumer.on('event.error', function (err) {
      if (err.code !== -1) {
        console.log('kafka consumer event.error', err);
      }
    });


    let errorHandler = function (func) {
      if (typeof func !== 'function') {
        throw new Error('func must be a function');
      }
      if (func.constructor.name === 'AsyncFunction') { // ES7 AsyncFunction
        func().catch(err => console.error('MQ handler error:', err));
      } else if (func.constructor.name === 'Function') { // Function
        try {
          func();
        } catch (e) {
          console.error('MQ handler error:', e);
        }
      }
    };

    // 使用 stream API, 避免消费节点被打爆
    stream.pipe(new Writable({
      write: function (chunk, encoding, callback) { // 实现顺序同步处理, 避免被大量消息打爆
        // 取出对应 topic 的 handler
        let handler = subscribedTopics[chunk.topic];

        if (handler.length === 1) { // 只传数据本身, 启动异步模式, 并自动完成读取和提交
          setTimeout(errorHandler(handler.bind(null, chunk)), 0);
          stream.consumer.commitMessage(chunk);
          callback();
        } else if (handler.length === 2) { // handler 只提供了 data, commit 两参数时, 提交的同时完成读取
          errorHandler(handler.bind(null, chunk, function () {
            stream.consumer.commitMessage(chunk);
            callback();
          }));
        } else { // 参数按照 data, commit, next 处理. 控制权交由用户
          errorHandler(handler.bind(null, chunk, () => stream.consumer.commitMessage(chunk), callback));
        }
      },
      objectMode: true
    }));

    consumerInstance = stream.consumer;
  }

  return consumerInstance;
}


function getProducer () {
  if (!options) {
    throw new Error('Kafka not init, please call .init() to initiate');
  }

  if (!getProducer.instance) {

    let producer = new Kafka.Producer({
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
      producer.on('ready', function () {
        console.log('kafka producer connect ok');
        resolve(producer);
      });
      producer.on('error', function (error) {
        console.error('error:', error);
        reject(error);
      });
    });

    producer.on('disconnected', function () {
      //断线自动重连
      console.error('disconnected');
      producer.connect();
    });
    producer.on('event.error', function (err) {
      //AliKafka服务器会主动掐掉空闲连接，如果发现这个异常，则客户端重连(先disconnect再connect)
      if (-1 !== err.code) {
        console.error(new Date(), 'event.error:', err);
      }
    });
    producer.on('delivery-report', function(err, report) {
      //消息发送成功，这里会收到report
      let reportCallback = producerWindow[report.key.toString()];
      reportCallback && reportCallback(report);
    });

    producer.setPollInterval(options.producerPollInterval);
    producer.connect();
    getProducer.instance = ready;
  }

  return getProducer.instance;
}

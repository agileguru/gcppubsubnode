require('dotenv').config();

const { PubSub } = require(`@google-cloud/pubsub`);

const pubsubClient = new PubSub();
const subscriptionName = 'doItTestTopicConsumer';
const timeoutMin = 2 * 60 * 1000;
const topicName = 'doItTestTopic';

const subscription = pubsubClient.subscription(subscriptionName);

let messageCount = 0;
let messageIdCount = new Map();

const messageHandler = message => {
    var date_ob = new Date();
    var day = ("0" + date_ob.getDate()).slice(-2);
    var month = ("0" + (date_ob.getMonth() + 1)).slice(-2);
    var year = date_ob.getFullYear();

    var date = year + "-" + month + "-" + day;
    var hours = date_ob.getHours();
    var minutes = date_ob.getMinutes();
    var seconds = date_ob.getSeconds();

    var dateTime = year + "-" + month + "-" + day + " " + hours + ":" + minutes + ":" + seconds;
    console.log(`message received ${message.id} with Data: ${message.data} at ${dateTime}` );
    if ( messageIdCount.has(message.id)) {
        count = messageIdCount.get(message.id);
        messageIdCount.set(message.id,count + 1);
    } else {
        messageIdCount.set(message.id,1);
    }
    messageCount += 1;
    message.ack();
};

subscription.on(`message`, messageHandler);
setTimeout(() => {
    subscription.removeListener('message', messageHandler);
    console.log(`${messageCount} message(s) received`);
    messageIdCount.forEach((count, id) => console.log(`Message ${id} had : count of ${count}`));
}, timeoutMin);
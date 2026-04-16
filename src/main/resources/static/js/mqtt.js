class MQTTMessageClient {

    constructor(host, port) {
        let _this = this;
        this.mqttConnected = false;
        this.topics = {};
        let d = new Date();
        this.clientID = d.getTime();
        
        this.client = new Paho.MQTT.Client(host, port, 'client' + this.clientID);
        this.client.onConnectionLost = this.onConnectionLost;

        this.setTopicMessageHandler = function (topic, messageHandler) {
            _this.topics[topic] = messageHandler;
        };

        let onMessageArrivedDefault = function (message) {
            let destinationName = message.destinationName;
            let payloadString = message.payloadString;

            try {
                _this.topics[destinationName](message);
            }
            catch (ex) {
                console.log('default onMessageArrived error : ', ex);
            }
        };

        this.client.onMessageArrived = onMessageArrivedDefault;
    }



    onConnectionLost(response) {
        this.mqttConnected = false;
        if (response.errorCode !== 0) {
            Notify.error('The connection to the subscription server was lost. error code : ' + response.errorCode);
        }
    }

    connect() {
        let _this = this;

        let onConnected = function () {
            
            _this.mqttConnected = true;

            for (let [topic, func] of Object.entries(_this.topics)) {
                // do something with key|value
                _this.client.subscribe(topic);
            }
            Notify.info('The connection to the subscription server was connected.');
        };

        let onFailure = function() {
            _this.mqttConnected = false;
            Notify.error('Failed to connect to subscription server.');
        };

        this.client.connect({ onSuccess: onConnected, onFailure: onFailure });
    }

    subscribe(topic) {
        this.client.subscribe(key);
    }

    disconnect() {
        this.client.disconnect();
    }

    publish(topic, payload) {
        let message = new Paho.MQTT.Message(payload);
        message.destinationName = topic;
        this.client.send(message);
    }
}


var amqp = require('amqplib/callback_api');
var conString = "amqp://sangram:sangram@localhost?heartbeat=60";

// if the connection is closed or fails to be established at all, we will reconnect
var amqpConn = null;
function start() {
    console.log("[AMQP] starting job publisher");
    amqp.connect(conString, function (err, conn) {
        if (err) {
            console.error("[AMQP]", err.message);
            return setTimeout(start, 1000);
        }
        conn.on("error", function (err) {
            if (err.message !== "Connection closing") {
                console.error("[AMQP] conn error", err.message);
            }
        });
        conn.on("close", function () {
            console.error("[AMQP] reconnecting");
            return setTimeout(start, 1000);
        });

        console.log("[AMQP] connected\n");
        amqpConn = conn;

        whenConnected();
    });
}

function whenConnected() {
    startPublisher();
}

var pubChannel = null;

//holds messages if failed to push into rabbitmq
var offlinePubQueue = [];

function startPublisher() {
    amqpConn.createConfirmChannel(function (err, ch) {
        if (closeOnErr(err)) return;
        ch.on("error", function (err) {
            console.error("[AMQP] channel error", err.message);
        });
        ch.on("close", function () {
            console.log("[AMQP] channel closed");
        });

        pubChannel = ch;
        while (true) {
            var m = offlinePubQueue.shift();
            if (!m) break;
            publish(m[0], m[1], m[2]);
        }
    });
}

// method to publish a message, will queue messages internally if the connection is down and resend later
function publish(exchange, routingKey, content) {
    try {
        pubChannel.publish(exchange, routingKey, content, { persistent: true },
            function (err, ok) {
                if (err) {
                    console.error("[AMQP] publish", err);
                    offlinePubQueue.push([exchange, routingKey, content]);
                    pubChannel.connection.close();
                }
            });
    } catch (e) {
        console.error("[AMQP] publish", e.message);
        offlinePubQueue.push([exchange, routingKey, content]);
    }
}

function closeOnErr(err) {
    if (!err) return false;
    console.error("[AMQP] error", err);
    amqpConn.close();
    return true;
}

var batch =1;

setInterval(function () {
    console.log('[AMQP] Starting New Batch :' + batch);

    for (var i = 0; i < 20; i++) {
        var msg = {
            sr_no: i,
            receiver: "sangram.desai@gmail.com",
            msg: "welcome onboard",
            sender: "sangram2681@gmail.com",
            notify_url: "http://localhost:3001/notifydelivery/mail",
            queue_time: new Date(),
            exec_time: "2017-08-13 14:00:00",
            expiry_time: "2017-08-13 18:00:00",
            retry_count: 0,
            batch:batch
        };
        var buffered_msg = new Buffer(JSON.stringify(msg));
        publish("", "jobs", buffered_msg);
    }
    console.log('[AMQP] Batch :' + batch + ' has been exhausted..\n');
    batch++;

}, 1000);

start();

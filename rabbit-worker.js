var amqp = require('amqplib/callback_api');
var conString = "amqp://sangram:sangram@localhost?heartbeat=60";

// if the connection is closed or fails to be established at all, we will reconnect
var amqpConn = null;
function start() {
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

        console.log("[AMQP] RabbitMq is connected");
        amqpConn = conn;

        whenConnected();
    });
}

function whenConnected() {
    startWorker();
}


//holds messages if failed to push into rabbitmq
var offlinePubQueue = [];

var consumerTag = null;
var last_batch = null;

var batch_threshold = 10;
var processed_batches = 0;
var prefetch_count =10;


// A worker that acks messages only if processed succesfully
function startWorker() {
    console.log("[AMQP] worker process is opening a communication channel...");
    amqpConn.createChannel(function (err, ch) {
        if (closeOnErr(err)) return;
        ch.on("error", function (err) {
            console.error("[AMQP] channel error", err.message);
        });
        ch.on("close", function () {
            console.log("[AMQP] channel closed");
        });
        ch.prefetch(prefetch_count);

        ch.assertQueue("jobs", { durable: true }, function (err, _ok) {
            if (closeOnErr(err)) return;

            console.log("[AMQP] worker process bind to 'jobs' queue...");
            ch.consume("jobs", processMsg, { noAck: false }, function (ch_err, ch_ok) {
                consumerTag = ch_ok.consumerTag;
                console.log("[AMQP] worker process has consumer tag " + consumerTag + "...");
            });

            console.log("[AMQP] worker process is listening for a job ...");
        });

        function processMsg(msg) {
            var msgJson = JSON.parse(msg.content.toString());
            var batch_no = parseInt(msgJson.batch);

            if (last_batch == null) {
                last_batch = batch_no;
                console.log("---------------------------BATCH:" + batch_no + "---------------------------------\n");
            } else {
                if (last_batch != batch_no) {
                    //processing of a batch completes here
                    processed_batches = processed_batches + 1;

                    console.log("---------------------------BATCH:" + last_batch + "---------------------------------\n");

                    console.log("[AMQP] worker process is processing new batch " + batch_no + "...");
                    console.log("[AMQP] worker process has processed " + processed_batches + " batches till  now...");
                    
                    console.log("---------------------------BATCH:" + batch_no + "---------------------------------\n");
                    last_batch = batch_no;
                }
            }

            if (processed_batches <= 10) {
                work(msg, function (ok) {
                    try {
                        if (ok) {
                            ch.ack(msg);
                        }
                        else {
                            console.log('rejecting' + msg);
                            ch.reject(msg, true);
                        }

                    } catch (e) {
                        closeOnErr(e);
                    }
                });
            } else {
                console.log("[AMQP] worker process ceasing on batch no threshold ....");
                ch.cancel(consumerTag);
            }
        }
    });
}

function work(msg, cb) {
    var msgJson = JSON.parse(msg.content.toString());
    var batch_no = parseInt(msgJson.batch);

    //console.log("[AMQP] Message Sr No:" + msgJson.sr_no + ", Batch: " + msgJson.batch + "\n" );
    //console.log("Got msg", msg.content.toString() + "\n \n");
    cb(true);
}

function closeOnErr(err) {
    if (!err) return false;
    console.error("[AMQP] error", err);
    amqpConn.close();
    return true;
}

start();

var amqp = require('amqplib/callback_api');
var conString = "amqp://sangram:sangram@localhost";



//producer
amqp.connect(conString, function (err, conn) {
    conn.createChannel(function (err, channel) {
        var queue = 'onboard_mailing_queue';
        console.log("Queueing messages in %s. To exit press CTRL+C", queue);

        for (var i = 0; i < 20; i++){
            var msg = {
                sr_no: i,
                receiver: "sangram.desai@gmail.com",
                msg: "welcome onboard",
                sender: "sangram2681@gmail.com",
                notify_url: "http://localhost:3001/notifydelivery/mail",
                queue_time: new Date(),
                exec_time: "2017-08-13 14:00:00",
                expiry_time: "2017-08-13 18:00:00",
                retry_count:0
            };
            
            var buffered_msg = new Buffer(JSON.stringify(msg));
            channel.assertQueue(queue, { durable: true });

            // Note: on Node 6 Buffer.from(msg) should be used
            channel.sendToQueue(queue, buffered_msg);
            console.log(" Msg Sent" + JSON.stringify(msg) + '\n');            
        }
    });

    //close the connection and exit
    setTimeout(function () {
        conn.close();
        process.exit(0)
    }, 500);
});
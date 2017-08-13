var amqp = require('amqplib/callback_api');
var conString = "amqp://sangram:sangram@localhost";


//producer
amqp.connect(conString, function (err, conn) {
    conn.createChannel(function (err, channel) {
        var queue = 'onboard_mailing_queue';
        channel.assertQueue(queue, { durable: true });
        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", queue);

        //acknowledge message recived
        channel.consume(queue,
            function (msg) {

                var msgJson = JSON.parse(msg.content.toString());
                var sr_no = parseInt(msgJson.sr_no);

                console.log('RECEIVED MESSAGES ::' + sr_no.toString());

                if (sr_no % 5 == 0) {
                    channel.ack(msg, false);//do not re-queue
                } else {
                    console.log('REJECTED ::' + sr_no.toString());
                    channel.noAck
                }
            },
            { noAck: false, consumerTag: "B" }
        );
    });

    //close the connection and exit
    /*setTimeout(function () {
        conn.close();
        process.exit(0)
    }, 500);*/
});
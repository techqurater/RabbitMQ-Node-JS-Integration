const express = require('express')
const amqp = require('amqplib')
const app = express()
const PORT = process.env.PORT || 3000
const connection_string = 'amqp://localhost'
const queue_name = 'test_queue'

app.get('/', (req, res) =>{
    res.send("<h1>Hello World !!!</h1>")
})

app.get('/publish', (req, res) =>{
    amqp.connect(connection_string).then((connection) => {
        connection.createChannel().then((channel) => {
            channel.assertQueue(queue_name, {durable: false}).then( () => {
              let date_ob = new Date();
              let day = ("0" + date_ob.getDate()).slice(-2);
              let month = ("0" + (date_ob.getMonth() + 1)).slice(-2);
              let year = date_ob.getFullYear();
                  
              let hours = date_ob.getHours();
              let minutes = date_ob.getMinutes();
              let seconds = date_ob.getSeconds();
                
              let dateTime = year + "-" + month + "-" + day + " " + hours + ":" + minutes + ":" + seconds;
              let msg = dateTime;
              channel.sendToQueue(queue_name, Buffer.from(msg));
              console.log(" [x] Published '%s'", msg.toString());
              res.send(`Message published ${msg}`)
            }
            ).catch((queueAssertionError) => {
              if(queueAssertionError){console.log(queueAssertionError);}
              channel.close();
              res.send("Queue assertion error")
            })
          }).catch((errChannel) => {
            if(errChannel){console.log(errChannel);}
            connection.close();
            res.send("Unable to create channel")
          })
    }).catch((connectionError) =>{
        console.log(`Connection error: ${connectionError}`);
        res.send('Unable to connect')
    })
})

app.get('/consume', (req, res) =>{
    amqp.connect(connection_string).then((connection) =>{
        connection.createChannel().then((channel) => {
          channel.assertQueue(queue_name, {durable: false}).then((_consumeOk) => {
            if(_consumeOk.messageCount > 0){
              channel.consume(queue_name, (message) => {
                console.log(" [x] Received '%s'", message.content.toString());
                channel.ack(message);
                res.send(` [x] Consumed ${message.content.toString()}`);
                channel.close();
              })
            }else{
              res.send(`No data found`);
            }
          }
          ).catch((queueAssertionError) => {
            if(queueAssertionError){console.log(queueAssertionError);}
            channel.close();
            res.send("Queue assertion error")
          })
        }).catch((errChannel) => {
          if(errChannel){console.log(errChannel);}
          connection.close();
          res.send("Unable to create channel")
        })
      }).catch((err) => {console.log(err);res.send("Unable to connect")})
})

app.listen(PORT, 'localhost', ()=>{
    console.log(`app is listning on http://localhost:${PORT}`);
})
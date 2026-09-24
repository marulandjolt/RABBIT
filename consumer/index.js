const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'cola_taller';

async function startConsumer() {
    let retries = 5;
    while (retries) {
        try {
            const connection = await amqp.connect(RABBITMQ_URL);
            const channel = await connection.createChannel();
            
            await channel.assertQueue(QUEUE_NAME, { durable: true });
            // Procesar de a 1 mensaje a la vez de forma justa
            channel.prefetch(1);

            console.log(`Consumidor esperando mensajes en la cola: ${QUEUE_NAME}`);

            channel.consume(QUEUE_NAME, (msg) => {
                if (msg !== null) {
                    const content = msg.content.toString();
                    const timestamp = new Date().toISOString();
                    
                    console.log(`[${timestamp}] Mensaje recibido: ${content}`);

                    // Confirmación manual (Acknowledgement) para retirar de la cola
                    channel.ack(msg);
                }
            }, { noAck: false });

            break;
        } catch (error) {
            retries -= 1;
            console.error(`Consumidor: Error conectando a RabbitMQ. Reintentos: ${retries}`, error.message);
            await new Promise(res => setTimeout(res, 5000));
        }
    }
}

startConsumer();

const express = require('express');
const amqp = require('amqplib');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const QUEUE_NAME = process.env.QUEUE_NAME || 'cola_taller';

let channel = null;

async function connectRabbitMQ() {
    let retries = 5;
    while (retries) {
        try {
            const connection = await amqp.connect(RABBITMQ_URL);
            channel = await connection.createChannel();
            // Declarar cola durable (persiste ante reinicios de RabbitMQ)
            await channel.assertQueue(QUEUE_NAME, { durable: true });
            console.log("Conectado exitosamente a RabbitMQ y cola declarada.");
            break;
        } catch (error) {
            retries -= 1;
            console.error(`Fallo al conectar a RabbitMQ. Reintentos restantes: ${retries}`, error.message);
            await new Promise(res => setTimeout(res, 5000));
        }
    }
}

app.post('/enviar', async (req, res) => {
    // Validar si el cuerpo es un JSON válido y no está vacío
    if (!req.body || Object.keys(req.body).length === 0) {
        res.setHeader('Content-Type', 'application/json');
        return res.status(400).json({ status: false, message: "Formato de mensaje inválido" });
    }

    try {
        if (!channel) {
            throw new Error("Canal de RabbitMQ no disponible");
        }

        const messageString = JSON.stringify(req.body);
        
        // Publicar mensaje con persistencia (persistent: true)
        channel.sendToQueue(QUEUE_NAME, Buffer.from(messageString), { persistent: true });

        res.setHeader('Content-Type', 'application/json');
        return res.status(200).json({ status: true, message: "Mensaje encolado" });
    } catch (error) {
        res.setHeader('Content-Type', 'application/json');
        return res.status(500).json({ status: false, message: "Error interno al publicar el mensaje" });
    }
});

app.listen(PORT, async () => {
    console.log(`Productor corriendo en el puerto ${PORT}`);
    await connectRabbitMQ();
});

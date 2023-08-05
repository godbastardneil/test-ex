const express = require('express');
const amqp = require("amqplib");

const PORT = process.env.PORT || 4001; // Порт сервера
const AMQP_SERVER = "amqp://localhost:5672"; // Сервер rabbitmq
const RPC_QUEUE = "rpc_queue"; // Очередь сервиса M2


const app = express(); // Создание сервера
app.use(express.json()); // добавление промежуточной функции разбирающий входящие запросы в объект в формате JSON


// Генерация случайного целого числа
const genId = () => { return Math.floor(Math.random()*1000); }


// Запуск сервека
const start = async () => {
    try {
        const connection = await amqp.connect(AMQP_SERVER); // подклучение к серверу
        const channel = await connection.createChannel(); // создание кнала
        
        // обработчик запроса маршрута /send
        app.get("/send", async (req, res) => {
            
            let data = req.body; // Получение тела запроса
            // если в теле нет id объекта, то добавить
            if(!('id' in data)) { data.id = genId(); }

            const correlationId = genId().toString(); // Генерация идентификатора корреляции
            
            console.log(` [x] Requesting fib ${correlationId}: `, data); // вывод отправляемой информации
            
            // Подтвердить существование очереди
            // С пустой строкой название генерируется случайный вариант
            // Возвращает саму очередь, из которых можно взять его название
            // Очередь с кастройной autoDelete, что значит, что с исчезновение получателей из этой очереди - она удаляется автоматически
            const q = await channel.assertQueue('', { autoDelete: true });

            // Отправление сообщение в сервис M2
            channel.sendToQueue(
                RPC_QUEUE, // Очереди сервиса M2
                Buffer.from(JSON.stringify(data)), // Передаваемый контент (отправка задания)
                {
                    correlationId: correlationId, // идентификатор корреляции
                    replyTo: q.queue // Название очереди, в которую неодходимо добавить ответ
                } // Параменты (Данные для возвращения результатов)
            );
            // Создание потребителя (получателя данных из очереди q)
            // Ожидается сообщение msg о завершении выполнении задания
            channel.consume(q.queue, (msg) => {
                // проверка идентификатора корреляции
                if (msg.properties.correlationId == correlationId) {
                    console.log(' [.] Got %s', msg.content.toString()); // вывод полученной информации
                    
                    // Закрытие этого потребителя (указание серверу на прекращение отправки сообщений потребителю)
                    // После чего удлится эта очередь
                    channel.cancel(msg.properties.correlationId); // Закрытие по тегу, указанному в отциях consume
                    
                    // возвращение работы сервисов пользователю
                    res.status(202).json({massage: msg.content.toString()});
                }
            }, { noAck: true, consumerTag: correlationId });
            // Опции Потребителя: noAck - не нужно явно указывать на получение отвера от сервиса отправителя
            // consumerTag - указание на очередь потребителя, что бы закрыть ее сразу после совершенной работы
        });

        // начинаем прослушивать подключения на PORT
        app.listen(PORT, () => console.log(`Server started on port ${PORT}`));
    } catch (e) {
        console.log(e)
        res.status(202).json({error: e.massage});
    }
}


start();
var express = require('express');
const amqp = require("amqplib");

const PORT = process.env.PORT || 4002; // Порт сервера
const AMQP_SERVER = "amqp://localhost:5672"; // Сервер rabbitmq
const RPC_QUEUE = "rpc_queue"; // Очередь сервиса M2

const app = express(); // Создание сервера
app.use(express.json()); // добавление промежуточной функции разбирающий входящие запросы в объект в формате JSON


// функция имитация деятельности с забержкой в 1 секунду
async function dataWork(data) {
    await new Promise((resolve) => setTimeout(resolve, 1000));
    
    // результат работы - сообщение о успешного 
    return (`Работа с данными ${data.id} завершена`);
}

// Запуск сервека
const start = async () => {
    try {

        const connection = await amqp.connect(AMQP_SERVER); // подклучение к серверу
        const channel = await connection.createChannel(); // создание кнала
        
        // Подтвердить существование очереди под именем RPC_QUEUE
        await channel.assertQueue(RPC_QUEUE);
        
        // Создание потребителя (получателя данных из очереди RPC_QUEUE)
        // Ожидается объет data для работы над ними
        channel.consume(RPC_QUEUE, async (data) => {
            const workData = JSON.parse(data.content); // парсинд контента (передаваемой информации)
            
            // вывод идентификатора корреляции и содержании объекта
            console.log(` [.] fib ${data.properties.correlationId}: `);
            console.dir(workData);

            // обработка объекта, и возвращение результата задачи
            dataWork(workData).then((ret) => {
                // вывод отправляемого результата
                console.log(` [x] fib ${data.properties.correlationId}: `, ret);
                // отправка результата выполнения задачи по свойству сообщения, в котором собержалось название очереди 
                channel.sendToQueue(
                    data.properties.replyTo, // название очереди
                    Buffer.from(ret), // результат выполнения задачи
                    {
                        correlationId: data.properties.correlationId // идентификатор корреляции
                    } // Параменты (Данные для проверки и закрытия очереди)
                );
                channel.ack(data); // Подтвердите данное сообщение или все сообщения до данного сообщения включительно
            })
            
            console.log(`END`); // конец обработки задачи
        });
        

        // начинаем прослушивать подключения на PORT
        app.listen(PORT, () => console.log(`Server started on port ${PORT}`));
    } catch (e) {
        console.log(e)
    }
}


start();
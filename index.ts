import { Kafka, type Consumer, type EachMessagePayload } from "kafkajs";

const kafka = new Kafka({
  clientId: "debezium-demo-client",
  brokers: ["localhost:9092"], // Replace with your Kafka broker addresses
});

const topic = new RegExp("dbserver1.inventory.*"); // Debezium topic (adjust as needed)

async function startDebeziumConsumer() {
  const consumer: Consumer = kafka.consumer({
    groupId: "debezium-demo-group",
  });

  await consumer.connect();
  console.log("Kafka Consumer connected");

  await consumer.subscribe({ topic });
  console.log(`Subscribed to topic ${topic}`);

  await consumer.run({
    eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
      const key = message.key?.toString();
      const value = message.value?.toString();

      if (value) {
        const parsedValue = JSON.parse(value);
        console.log("Received Debezium event:", { key, value: parsedValue });

        // Access Debezium payload for CDC details
        const { payload } = parsedValue;
        if (payload) {
          console.log("Operation:", payload.op); // e.g., "c" for create, "u" for update, "d" for delete
          console.log("Data After:", payload.after); // Data after operation
          console.log("Data Before:", payload.before); // Data before operation (for delete)
        }
      }
    },
  });
}

startDebeziumConsumer().catch(console.error);

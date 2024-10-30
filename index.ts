import {
  KafkaConsumer,
  type ConsumerGlobalConfig,
  type ConsumerTopicConfig,
  type Message,
} from "node-rdkafka";

const consumerConfig: ConsumerGlobalConfig = {
  "group.id": "demo-group",
  "metadata.broker.list": "localhost:9092", // Update with your Kafka broker's address
};

const topicConfig: ConsumerTopicConfig = {
  "auto.offset.reset": "earliest",
};

// Function to handle messages from Kafka
function startDebeziumConsumer() {
  const consumer = new KafkaConsumer(consumerConfig, topicConfig);

  // Set up event handlers
  consumer
    .on("ready", () => {
      console.log("Consumer is ready and listening for messages...");
      consumer.subscribe(["dbserver1.inventory.customers"]); // Replace with your Debezium topic(s)
      consumer.consume(); // Start consuming messages
    })
    .on("data", (data: Message) => {
      const key = data.key ? data.key.toString() : null;
      const value = data.value ? JSON.parse(data.value.toString()) : null;

      console.log("Received Debezium event:");
      console.log("Key:", key);
      console.log("Value:", value);

      // Here, you can process the change event data as needed
      if (value && value.payload) {
        console.log("Operation:", value.payload.op); // e.g., "c" for create, "u" for update, "d" for delete
        console.log("Data:", value.payload.after || value.payload.before);
      }
    })
    .on("event.error", (err) => {
      console.error("Error from consumer:", err);
    });

  consumer.connect();
}

// Start the consumer
startDebeziumConsumer();

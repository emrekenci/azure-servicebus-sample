namespace ServiceBus
{
    using System;
    using System.Configuration;
    using System.Text;

    using Microsoft.Azure.ServiceBus;
    using Newtonsoft.Json;

    using ServiceBusDemo.Models;

    internal class WriteMessagesProgram
    {
        private static IQueueClient _queueClient;

        private static void Main()
        {
            string connectionString = ConfigurationManager.AppSettings["ConnectionString"];

            string queueName = ConfigurationManager.AppSettings["QueueName"];

            _queueClient = new QueueClient(connectionString, queueName);

            /*
             * Write 10 messages without a sessionId.
             * These messages will be consumed in random order.
             *
             * IF SESSION SUPPORT IS ENABLED ON THE QUEUE, THIS CALL WILL THROW AN EXCEPTION
             * Queues that have session support enabled cannot receive messages with null SessionId value.
             * Make sure you provide a sessionId value if you're using a session enabled queue.
             *
             */
            WriteMessages(10);

            /*
             * Write 10 messages with same sessionId = "session-0".
             * These messages will be delivered to the consumer in the order they were written to the queue.
             */
            WriteMessages(10, "session-0");

            Console.ReadLine();
        }

        /// <summary>
        /// Writes the given number of messages to the queue.
        /// The messages we're writing are of the model SmsMessage, replace it with your model.
        /// </summary>
        private static async void WriteMessages(int numberOfMessages, string sessionId = null)
        {
            for (int i = 0; i < numberOfMessages; i++)
            {
                // Construct the model
                var queueMessage = new SmsMessage
                {
                    Content = "Random SMS content " + i,
                    Destination = "+905012345678"
                };

                // Convert it to JSON
                string messageJson = JsonConvert.SerializeObject(queueMessage);

                // Convert JSON to byte[]
                byte[] messageBytes = Encoding.ASCII.GetBytes(messageJson);

                // Construct service bus message
                var message = new Message(messageBytes)
                {
                    Label = Guid.NewGuid().ToString(), // Optional label

                    // Queues that have session support enabled cannot receive messages with null SessionId value.
                    // Make sure you provide a sessionId value if you're using a session enabled queue.
                    SessionId = sessionId
                };

                // Send the message to the queue
                await _queueClient.SendAsync(message);

                Console.WriteLine("Wrote Message " + i);
            }
        }
    }
}

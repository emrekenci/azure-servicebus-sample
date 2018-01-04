namespace ServiceBusDemo.ConsumeMessages
{
    using System;
    using System.Configuration;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;

    using ServiceBusDemo.Models;

    internal class ConsumeMessagesProgram
    {
        private static IQueueClient _queueClient;

        private static IMessageReceiver _dlqReceiver;

        private static MessageHandlerOptions _handlerOptions;

        private static void Main()
        {
            string connectionString = ConfigurationManager.AppSettings["ConnectionString"];

            string queueName = ConfigurationManager.AppSettings["QueueName"];
            
            _queueClient = new QueueClient(connectionString, queueName);

            _dlqReceiver = new MessageReceiver(connectionString, EntityNameHelper.FormatDeadLetterPath(queueName));
            
            // Configure the MessageHandler Options in terms of exception handling, number of concurrent messages to deliver etc.
            _handlerOptions = new MessageHandlerOptions(ExceptionHandler)
            {
                // Maximum number of Concurrent calls to the callback `HandleMessage`, set to 1 for simplicity.
                // Set it according to how many messages the application wants to process in parallel.
                MaxConcurrentCalls = 1,

                // Indicates whether MessagePump should automatically complete the messages after returning from User Callback.
                // False below indicates the Complete will be handled by the User Callback as in `HandleMessage` below.
                AutoComplete = false
            };

            /*
             * Consume messages from a session support disabled queue.
             */
            ConsumeMessages();

            /*
             * Consume messages from a session support enabled queue.
             * Consumes messages from every session on the queue.
             */
            ConsumeSessionedMessages();

            // Consume deadletter queue messages.
            _dlqReceiver.RegisterMessageHandler(HandleMessage, _handlerOptions);

            Console.ReadLine();
        }

        private static void ConsumeMessages()
        {
            // Register the function that will process messages. This is for un-sessioned messages
            _queueClient.RegisterMessageHandler(HandleMessage, _handlerOptions);
        }

        private static void ConsumeSessionedMessages()
        {
            // Similar to MessageHandlerOptions just different for sessions.
            var sessionHandlerOptions = new SessionHandlerOptions(ExceptionHandler)
            {
                AutoComplete = false,
                MaxConcurrentSessions = 1
            };

            // This is for sessioned messages
            _queueClient.RegisterSessionHandler(ProcessSessionMessageAsync, sessionHandlerOptions);
        }

        private static async Task HandleMessage(Message message, CancellationToken token)
        {
            try
            {
                bool isDeadLetterMessage = message.UserProperties.Keys.FirstOrDefault(k => k.Equals("DeadLetterReason")) != null;

                bool hasSessionId = !string.IsNullOrEmpty(message.SessionId);

                Console.WriteLine("Message Received!");

                var smsMessage = JsonConvert.DeserializeObject<SmsMessage>(Encoding.UTF8.GetString(message.Body));

                Console.WriteLine("Content: " + smsMessage.Content);
                Console.WriteLine("Destination: " + smsMessage.Destination);

                Console.WriteLine("Is DLQ: " + isDeadLetterMessage);

                // Writing messages with not null sessionId's to session disabled queues is not forbidden.
                // In fact, if you write messages with sessionId to a queue where sessions are disabled,
                // you will still get messages FIFO.
                Console.WriteLine("Has session ID: " + hasSessionId);

                if (hasSessionId)
                {
                    Console.WriteLine("Session ID: " + message.SessionId);
                }

                // Complete the message so that it is not received again.
                // This can be done only if the queue Client is created in ReceiveMode.PeekLock mode (which is the default).
                if (isDeadLetterMessage)
                {
                    await _dlqReceiver.CompleteAsync(message.SystemProperties.LockToken);
                }
                else
                {
                    await _queueClient.CompleteAsync(message.SystemProperties.LockToken);
                }

                Console.WriteLine("***********************\n");
            }
            catch
            {
                Console.WriteLine("Received exception while handling the message, abandoning...");

                await _queueClient.AbandonAsync(message.SystemProperties.LockToken);
            }
        }

        private static async Task ProcessSessionMessageAsync(IMessageSession session, Message message, CancellationToken token)
        {
            // Process the message.
            Console.WriteLine($"SequenceNumber: {message.SystemProperties.SequenceNumber} Body: {Encoding.UTF8.GetString(message.Body)}");
            Console.WriteLine($"Session Id: {session.SessionId}");
            Console.WriteLine("********************************");

            await session.CompleteAsync(message.SystemProperties.LockToken);
        }

        private static Task ExceptionHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            var context = exceptionReceivedEventArgs.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troubleshooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Executing Action: {context.Action}");
            return Task.CompletedTask;
        }
    }
}

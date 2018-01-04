namespace ServiceBusDemo.Models
{
    /// <summary>
    /// We write and read messages in this format.
    /// Replace this with any model for your needs.
    /// </summary>
    public class SmsMessage
    {
        public string Content { get; set; }

        public string Destination { get; set; }
    }
}

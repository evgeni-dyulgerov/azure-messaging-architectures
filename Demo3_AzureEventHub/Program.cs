using Azure.Messaging.EventHubs.Producer;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Demo3_AzureEventHub
{
    public class Program
    {
        private const string connectionString = "Endpoint=sb://demo3hubs.servicebus.windows.net/;SharedAccessKeyName=MyPolicy;SharedAccessKey=Q4JBCXECKZn6c/mtrWoAnEV/Ei2O+n9s+6OIlxudcS8=";
        private const string eventHubName = "myeventhub";

        static void Main(string[] args)
        {
            Task.Run(async () =>
            {
                await using var producerClient = new EventHubProducerClient(connectionString, eventHubName);

                using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

                eventBatch.TryAdd(new Azure.Messaging.EventHubs.EventData(Encoding.UTF8.GetBytes("First event")));
                eventBatch.TryAdd(new Azure.Messaging.EventHubs.EventData(Encoding.UTF8.GetBytes("Second event")));
                eventBatch.TryAdd(new Azure.Messaging.EventHubs.EventData(Encoding.UTF8.GetBytes("Third event")));

                await producerClient.SendAsync(eventBatch);

                Console.WriteLine("A batch of 3 events has been published.");
                // Do any async anything you need here without worry
            }).GetAwaiter().GetResult();
        }
    }
}

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;
using System;
using System.Text;
using System.Threading.Tasks;

namespace Demo3_ReceiveEvents
{
    public class Program
    {
        private const string connectionString = "Endpoint=sb://demo3hubs.servicebus.windows.net/;SharedAccessKeyName=MyPolicy;SharedAccessKey=Q4JBCXECKZn6c/mtrWoAnEV/Ei2O+n9s+6OIlxudcS8=";
        private const string eventHubName = "myeventhub";
        private const string blobStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=demoblob3;AccountKey=FBN2BNYroEiIGSJPbwvPuikhUMmgdwzJm6UpsZW5hxRtoMFZyPMtqsJriPGuW4i4/uvClwVQD8sk+AStDVALLQ==;EndpointSuffix=core.windows.net";
        private const string blobContainerName = "eventhubcontainer";

        static void Main(string[] args)
        {
            Task.Run(async () =>
            {
                string consumerGroup = EventHubConsumerClient.DefaultConsumerGroupName;

                // Create a blob container client that the event processor will use 
                BlobContainerClient storageClient = new BlobContainerClient(blobStorageConnectionString, blobContainerName);

                // Create an event processor client to process events in the event hub
                EventProcessorClient processor = new EventProcessorClient(storageClient, consumerGroup, connectionString, eventHubName);

                // Register handlers for processing events and handling errors
                processor.ProcessEventAsync += ProcessEventHandler;
                processor.ProcessErrorAsync += ProcessErrorHandler;

                // Start the processing
                await processor.StartProcessingAsync();

                // Wait for 10 seconds for the events to be processed
                await Task.Delay(TimeSpan.FromSeconds(10));

                // Stop the processing
                await processor.StopProcessingAsync();
            }).GetAwaiter().GetResult();
        }
        static async Task ProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Write the body of the event to the console window
            Console.WriteLine("\tRecevied event: {0}", Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray()));

            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        static Task ProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Console.WriteLine($"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered. This was not expected to happen.");
            Console.WriteLine(eventArgs.Exception.Message);
            return Task.CompletedTask;
        }
    }
}

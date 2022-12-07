using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Demo3_AzureEventHub
{
    public class EventProducer
    {
        string connectionString = "Endpoint=sb://demo3hubs.servicebus.windows.net/;SharedAccessKeyName=MyEventHubsPolicy;SharedAccessKey=Oe6Gi57OVp1UH8QpIxSXF7rq6ZbZ512pSG8GZZpH4PQ=";
        string eventHubName = "myeventhub";
        EventDataBatch generateData;
        List<string> device = new List<string>();
        EventHubProducerClient producerClient;
        public void Init()
        {
            producerClient = new EventHubProducerClient(connectionString, eventHubName);
            device.Add("Mobile");
            device.Add("Laptop");
            device.Add("Desktop");
            device.Add("Tablet");
        }
        public async Task GenerateEvent()
        {
            try
            {
                // send in batch  
                int partitionId = 0;
                foreach (var eachDevice in device)
                {
                    StringBuilder strBuilder = new StringBuilder();
                    var batchOptions = new CreateBatchOptions()
                    {
                        PartitionId = partitionId.ToString()
                    };
                    generateData = producerClient.CreateBatchAsync(batchOptions).Result;
                    strBuilder.AppendFormat("Search triggered for iPhone 21 from decive {0} ", eachDevice);
                    var eveData = new EventData(Encoding.UTF8.GetBytes(strBuilder.ToString()));
                    // All value should be dynamic  
                    eveData.Properties.Add("UserId", "UserId");
                    eveData.Properties.Add("Location", "North India");
                    eveData.Properties.Add("DeviceType", eachDevice);
                    generateData.TryAdd(eveData);
                    producerClient.SendAsync(generateData).Wait();
                    //Reset partitionId as it can be 0 or 1 as we have define in azure event hub  
                    partitionId++;
                    if (partitionId > 1) partitionId = 0;
                }
                await Task.CompletedTask;
            }
            catch (Exception exp)
            {
                Console.WriteLine("Error occruied {0}. Try again later", exp.Message);
            }
        }
    }
}

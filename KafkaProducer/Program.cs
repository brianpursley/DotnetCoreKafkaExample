using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaProducer
{
    class Program
    {
        public static async Task Main()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
            
            // Create a producer that can be used to send messages to kafka that have no key and a value of type string 
            using var p = new ProducerBuilder<Null, string>(config).Build();

            var i = 0;
            while (true)
            {
                // Construct the message to send (generic type must match what was used above when creating the producer)
                var message = new Message<Null, string>
                {
                    Value = $"Message #{++i}"
                };
                
                // Send the message to our test topic in Kafka
                var dr = await p.ProduceAsync("test", message);
                Console.WriteLine($"Delivered '{dr.Value}' to topic {dr.Topic}, partition {dr.Partition}, offset {dr.Offset}");
                
                Thread.Sleep(5000);
            }
        }
    }
}

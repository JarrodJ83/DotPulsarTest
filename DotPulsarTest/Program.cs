using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Pulsar.Client.Api;
using PulsarClient = DotPulsar.PulsarClient;

namespace DotPulsarTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            const string myTopic = "persistent://public/default/mytopic";
            
            var tasks = new List<Task>();
            tasks.Add(UsePulsarClient(myTopic, cts.Token));
            tasks.Add(UseDotPulsar(myTopic, cts.Token));

            Task.WaitAll(tasks.ToArray());

            Console.ReadKey();

            cts.Cancel();
        }

        static async Task UsePulsarClient(string topic, CancellationToken cancel)
        {
            var client = new PulsarClientBuilder()
                .ServiceUrl("pulsar://pulsar:6650")
                .Build();

            var producer = await client.NewProducer()
                .Topic(topic)
                .CreateAsync();

            var consumer = await client.NewConsumer()
                .Topic(topic)
                .SubscriptionName("sub")
                .SubscribeAsync();

            var messageId = await producer.SendAsync(Encoding.UTF8.GetBytes($"Sent from C# at '{DateTime.Now}'"));
            Console.WriteLine($"MessageId is: '{messageId}'");
            
            var message = await consumer.ReceiveAsync();
            Console.WriteLine($"Received: {Encoding.UTF8.GetString(message.Data)}");

            await consumer.AcknowledgeAsync(message.MessageId);
        }

        static async Task UseDotPulsar(string topic, CancellationToken cancel)
        {
            await using var client = PulsarClient.Builder().ExceptionHandler(async context =>
                {
                    Console.WriteLine(context.Exception?.ToString());
                })
                .ServiceUrl(new Uri("pulsar://pulsar:6650"))
                .Build();

            var producer = client.NewProducer()
                .Topic(topic)
                .Create();
            
            Monitor(producer, cancel);

            _ = await producer.Send(Encoding.UTF8.GetBytes("Hello World"), cancel);

            var consumer = client.NewConsumer()
                .SubscriptionName("MySubscription")
                .Topic(topic)
                .Create();

            await foreach (var message in consumer.Messages())
            {
                Console.WriteLine("Received: " + Encoding.UTF8.GetString(message.Data.ToArray()));
                await consumer.Acknowledge(message, cancel);
            }
        }

        private static async ValueTask Monitor(IProducer producer, CancellationToken cancellationToken)
        {
            var state = ProducerState.Disconnected;

            while (!cancellationToken.IsCancellationRequested)
            {
                state = await producer.StateChangedFrom(state, cancellationToken);

                var stateMessage = state switch
                {
                    ProducerState.Connected => $"The producer is connected",
                    ProducerState.Disconnected => $"The producer is disconnected",
                    ProducerState.Closed => $"The producer has closed",
                    ProducerState.Faulted => $"The producer has faulted",
                    _ => $"The producer has an unknown state '{state}'"
                };

                Console.WriteLine(stateMessage);

                if (producer.IsFinalState(state))
                    return;
            }
        }
    }
}

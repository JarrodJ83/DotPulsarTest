using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;

namespace DotPulsarTest
{
    class Program
    {
        static async Task Main(string[] args)
        {
            const string myTopic = "persistent://public/default/mytopic";

            await using var client = PulsarClient.Builder()
                .ServiceUrl(new Uri("pulsar://pulsar:6650"))
                .Build(); //Connecting to pulsar://localhost:6650

            var producer = client.NewProducer()
                .Topic(myTopic)
                .Create();

            CancellationTokenSource cts = new CancellationTokenSource();

            Monitor(producer, cts.Token);

            _ = await producer.Send(Encoding.UTF8.GetBytes("Hello World"));

            var consumer = client.NewConsumer()
                .SubscriptionName("MySubscription")
                .Topic(myTopic)
                .Create();

            await foreach (var message in consumer.Messages())
            {
                Console.WriteLine("Received: " + Encoding.UTF8.GetString(message.Data.ToArray()));
                await consumer.Acknowledge(message);
            }
            cts.Cancel();
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

using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using NUnit.Framework.Internal.Execution;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests;

public class Reproducer344Tests
{
    public class Event
    {
        public string Order { get; set; }
        public string Version { get; set; }
    }
    
    public class Eventv3
    {
        public int Order { get; set; }
        public string Version { get; set; }
    }
    
    public class EventEnvelope
    {
        public string UUID{ get; set; }
        public string Id { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class EventEnvelope<T> : EventEnvelope
    {
        public T Event { get; set; }
    }

    public class AggregateEvent
    {
        public EventEnvelope StreamEvent { get; set; }
        public string Order { get; set; }
        public string Version { get; set; }
    }

    private class Reproducer344TestsProcessor
    {
        public static async Task<EventEnvelope<Eventv3>> ProcessRawMessage(string id)
        {
            var rawMessage = id;
            try
            {
                var v2Envelope = JsonConvert.DeserializeObject<EventEnvelope<Event>>(rawMessage);
                //External awaited mapper call
                var v3Envelope = MapToV3(v2Envelope);
                rawMessage = JsonConvert.SerializeObject(v3Envelope.Result);

                return await Task.FromResult(JsonConvert.DeserializeObject<EventEnvelope<Eventv3>>(rawMessage));
            }
            catch (Exception e)
            {
                return await Task.FromResult(JsonConvert.DeserializeObject<EventEnvelope<Eventv3>>(rawMessage));
            }
        }
    }
    
    public static async Task<EventEnvelope<Eventv3>> MapToV3(EventEnvelope<Event> v2Envelope)
    {
        return await Task.FromResult(new EventEnvelope<Eventv3>
        {
            Id = v2Envelope.Id,
            Timestamp = v2Envelope.Timestamp,
            UUID = v2Envelope.UUID,
            Event = new Eventv3
            {
                Order = int.Parse("123"),
                Version = v2Envelope.Event.Version
            }
        });
    }
    
    private Topology GetStreamTopology()
    {
        var eventProcessor = new Reproducer344TestsProcessor();
        var builder = new StreamBuilder();
    
        var stream = builder
            .Stream<string, string>("test-input")
            .MapValuesAsync(
                async (record, _) =>
                {
                    var test = await Reproducer344TestsProcessor.ProcessRawMessage(record.Value);
                    return test;
                }, null,
                new RequestSerDes<string, string>(new StringSerDes(), new StringSerDes()),
                new ResponseSerDes<string, EventEnvelope<Eventv3>>(new StringSerDes(),
                    new JsonSerDes<EventEnvelope<Eventv3>>()));

        var table1 = builder
            .GlobalTable("test-table1", 
                new StringSerDes(),
                new JsonSerDes<EventEnvelope<Event>>(),
                InMemory.As<string, EventEnvelope<Event>>("test-table1-store")
                    .WithValueSerdes<JsonSerDes<EventEnvelope<Event>>>());

        var table2 = builder
            .GlobalTable("test-table2", 
                new StringSerDes(),
                new JsonSerDes<EventEnvelope<Event>>(),
                InMemory.As<string, EventEnvelope<Event>>("test-table2-store")
                    .WithValueSerdes<JsonSerDes<EventEnvelope<Event>>>());

        stream.Join(table1,
                (_, value) => value.Id,
                (value1, value2) =>
                {
                    var aggregateEvent = new AggregateEvent
                    {
                        StreamEvent = value1,
                        Order = value2.Event.Order
                    };

                    return aggregateEvent;
                })
            .Join(table2,
                (_, value) => value.StreamEvent.Id,
                (aggregateEvent, event2) =>
                {
                    aggregateEvent.Version = event2.Event.Version;
                    return aggregateEvent;
                })
            .Filter((_, value) => value.Order != null && value.Version != null)
            .MapValues((_, value) => value)
            .To<StringSerDes, JsonSerDes<AggregateEvent>>("test-output");

        return builder.Build();
    }
    
    [Test]
    public void CreateStream_ProducesOrderToOutputTopic()
    {
        var stream = GetStreamTopology();
        var config = new StreamConfig<StringSerDes, StringSerDes>();
        config.ApplicationId = "test-reproducer-344";
        config.BootstrapServers = "localhost:9092";
        
        using var driver = new TopologyTestDriver(stream, config);
        var inputTopic = driver.CreateInputTopic<string, string>($"test-input");
        var table1Topic = driver.CreateInputTopic<string, EventEnvelope<Event>, StringSerDes, JsonSerDes<EventEnvelope<Event>>>("test-table1");
        var table2Topic = driver.CreateInputTopic<string, EventEnvelope<Event>, StringSerDes, JsonSerDes<EventEnvelope<Event>>>("test-table2");
        var outputTopic = driver.CreateOuputTopic<string, AggregateEvent, StringSerDes, JsonSerDes<AggregateEvent>>($"test-output");
        
        table1Topic.PipeInput("id", 
            new EventEnvelope<Event>()
            {
                Id = "id", Timestamp = DateTime.Now, UUID = Guid.NewGuid().ToString(),
                Event = new Event()
                {
                    Order = "order1"
                }
            });
        
        table2Topic.PipeInput("id",
            new EventEnvelope<Event>()
            {
                Id = "id", Timestamp = DateTime.Now, UUID = Guid.NewGuid().ToString(),
                Event = new Event {
                    Version = "v3"
                }
            });
        
        inputTopic.PipeInput("id", JsonConvert.SerializeObject(new EventEnvelope<Event>()
        {
            Id = "id",
            Timestamp = DateTime.Now,
            UUID = Guid.NewGuid().ToString(),
            Event = new Event
            {
                Version = "v3"
            }
        }));

        var output = outputTopic.ReadKeyValue();
        Assert.IsNotNull(output);
        Assert.AreEqual("v3", output.Message.Value.Version);
        Assert.AreEqual("order1", output.Message.Value.Order);
    }
}
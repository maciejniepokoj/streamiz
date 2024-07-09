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
        public async Task<EventEnvelope> ProcessRawMessage(string id)
        {
            var newEvent = new EventEnvelope();
            newEvent.Id = id;
            newEvent.Timestamp = DateTime.Now;
            newEvent.UUID = Guid.NewGuid().ToString();
            return await Task.FromResult(newEvent);
        }
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
                    var test = await eventProcessor.ProcessRawMessage(record.Value);
                    return test;
                }, null,
                new RequestSerDes<string, string>(new StringSerDes(), new StringSerDes()),
                new ResponseSerDes<string, EventEnvelope>(new StringSerDes(),
                    new JsonSerDes<EventEnvelope>()));

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
        
        inputTopic.PipeInput("id", "id");

        var output = outputTopic.ReadKeyValue();
        Assert.AreEqual("v3", output.Message.Value.Version);
        Assert.AreEqual("order1", output.Message.Value.Order);
    }
}
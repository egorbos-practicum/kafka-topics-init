using System.CommandLine;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

var bootstrapServerOption = new Option<string>("--bootstrap-server")
{
    Description = "Kafka bootstrap server"
};
var topicOption = new Option<string>("--topic")
{
    Description = "Topic name"
};
var partitionsOption = new Option<int>("--partitions")
{
    Description = "Number of partitions",
    DefaultValueFactory = _ => 1
};
var replicationFactorOption = new Option<short>("--replication-factor")
{
    Description = "Replication factor",
    DefaultValueFactory = _ => 1
};

var rootCommand = new RootCommand("Kafka topic creator");
rootCommand.Options.Add(bootstrapServerOption);
rootCommand.Options.Add(topicOption);
rootCommand.Options.Add(partitionsOption);
rootCommand.Options.Add(replicationFactorOption);

rootCommand.SetAction(parseResult =>
{
    var server = parseResult.GetValue(bootstrapServerOption);
    var topic = parseResult.GetValue(topicOption);
    var partitionsCount = parseResult.GetValue(partitionsOption);
    var replicationFactor = parseResult.GetValue(replicationFactorOption);

    var adminClientConfig = new AdminClientConfig
    {
        BootstrapServers = server
    };

    var adminClient = new AdminClientBuilder(adminClientConfig).Build();
    var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));

    if (metadata.Topics.Count == 0)
    {
        adminClient.CreateTopicsAsync([
            new TopicSpecification { Name = topic, NumPartitions = partitionsCount, ReplicationFactor = replicationFactor }
        ]).Wait();
    }
});

var result = rootCommand.Parse(args);
return result.Invoke();
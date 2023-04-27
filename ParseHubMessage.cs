using System;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace EventHubsFunctionTest
{
    public class ParseHubMessage
    {
        private readonly ILogger _logger;

        public ParseHubMessage(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ParseHubMessage>();
        }

        [Function("ParseHubMessage")]
        public void Run([EventHubTrigger("iothub-ehub-fegontesth-23930623-74e1d9c4d0", Connection = "FegonTestHub_events_IOTHUB", IsBatched = false)] EventData data)
        {
            _logger.LogInformation("Got ContentType {ContentType}", data.ContentType);
            _logger.LogInformation("Got CorrelationId {CorrelationId}", data.CorrelationId);
            _logger.LogInformation("Got EnqueuedTime {EnqueuedTime}", data.EnqueuedTime);
            _logger.LogInformation("Got EventBody {EventBody}", data.EventBody);
            _logger.LogInformation("Got MessageId {MessageId}", data.MessageId);
            _logger.LogInformation("Got PartitionKey {PartitionKey}", data.PartitionKey);
            _logger.LogInformation("Got SequenceNumber {SequenceNumber}", data.SequenceNumber);

            foreach(var property in data.Properties)
            {
                _logger.LogInformation("Got property {Key} with value {Value}", property.Key, property.Value);
            }

            foreach (var systemProperty in data.SystemProperties)
            {
                _logger.LogInformation("Got system property {Key} with value {Value}", systemProperty.Key, systemProperty.Value);
            }

            var rawMessage = data.GetRawAmqpMessage();

            foreach(var applicationProperty in rawMessage.ApplicationProperties)
            {
                _logger.LogInformation("Got raw application property {Key} with value {Value}", applicationProperty.Key, applicationProperty.Value);
            }

            foreach (var deliveryAnnotation in rawMessage.DeliveryAnnotations)
            {
                _logger.LogInformation("Got raw delivery annotation {Key} with value {Value}", deliveryAnnotation.Key, deliveryAnnotation.Value);
            }

            foreach (var footer in rawMessage.Footer)
            {
                _logger.LogInformation("Got raw footer {Key} with value {Value}", footer.Key, footer.Value);
            }

            _logger.LogInformation("Got raw body -> BodyType {BodyType}", rawMessage.Body.BodyType);

            _logger.LogInformation("Got raw header DeliveryCount {DeliveryCount}", rawMessage.Header.DeliveryCount);
            _logger.LogInformation("Got raw Header Durable {Durable}", rawMessage.Header.Durable);
            _logger.LogInformation("Got raw Header FirstAcquirer {FirstAcquirer}", rawMessage.Header.FirstAcquirer);
            _logger.LogInformation("Got raw Header Priority {Priority}", rawMessage.Header.Priority);

            _logger.LogInformation("Got raw Properties AbsoluteExpiryTime {AbsoluteExpiryTime}", rawMessage.Properties.AbsoluteExpiryTime);
            _logger.LogInformation("Got raw Properties ContentEncoding {ContentEncoding}", rawMessage.Properties.ContentEncoding);
            _logger.LogInformation("Got raw Properties ContentType {ContentType}", rawMessage.Properties.ContentType);
            _logger.LogInformation("Got raw Properties CorrelationId {CorrelationId}", rawMessage.Properties.CorrelationId);
            _logger.LogInformation("Got raw Properties CreationTime {CreationTime}", rawMessage.Properties.CreationTime);
            _logger.LogInformation("Got raw Properties GroupId {GroupId}", rawMessage.Properties.GroupId);
            _logger.LogInformation("Got raw Properties GroupSequence {GroupSequence}", rawMessage.Properties.GroupSequence);
            _logger.LogInformation("Got raw Properties MessageId {MessageId}", rawMessage.Properties.MessageId);
            _logger.LogInformation("Got raw Properties ReplyTo {ReplyTo}", rawMessage.Properties.ReplyTo);
            _logger.LogInformation("Got raw Properties ReplyToGroupId {ReplyToGroupId}", rawMessage.Properties.ReplyToGroupId);
            _logger.LogInformation("Got raw Properties Subject {Subject}", rawMessage.Properties.Subject);
            _logger.LogInformation("Got raw Properties To {To}", rawMessage.Properties.To);
            _logger.LogInformation("Got raw Properties UserId {UserId}", rawMessage.Properties.UserId);
        }
    }
}

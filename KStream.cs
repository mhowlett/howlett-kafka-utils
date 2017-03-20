using System;
using System.Collections;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Howlett.Kafka.Utils
{
    /// <summary>
    ///     allow iterating over stream.
    /// </summary>
    public class KStream<K, V> : IEnumerable<KeyValuePair<K, V>>, IDisposable
    {
        Consumer<K,V> consumer;

        public KStream(
            string bootstrapServers,
            string topic,
            int minPartition, int maxPartition,
            IDeserializer<K> keyDeserializer, 
            IDeserializer<V> valueDeserializer
        )
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "api.version.request", true },
                { "group.id", Guid.NewGuid().ToString() }, // shouldn't matter.
                { "enable.auto.commit", false }
            };

            consumer = new Consumer<K, V>(config, keyDeserializer, valueDeserializer);
        }

        public void Dispose()
        {
            consumer.Dispose();
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}

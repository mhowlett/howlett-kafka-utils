using System;
using System.Collections;
using System.Collections.Generic;
using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace Howlett.Kafka.Utils
{
    /// <summary>
    ///     K,V table materialized from a stream.
    /// </summary>
    public class KTable<K, V, MK, MV> : IEnumerable<KeyValuePair<K, V>>
    {
        private Dictionary<K, V> d;

        /// <summary>
        ///     Constructor
        /// </summary>
        /// <param name="bootstrapServers">
        ///     bootstrap servers
        /// </param>
        /// <param name="topic">
        ///     topic
        /// </param>
        /// <param name="minPartition">
        ///     min partition
        /// </param>
        /// <param name="maxPartition">
        ///     max partition
        /// </param>
        /// <param name="keyDeserializer">
        ///     key deserializer
        /// </param>
        /// <param name="valueDeserializer">
        ///     value deserializer
        /// </param>
        /// <param name="kConstruct">
        ///     to make dict key
        /// </param>
        /// <param name="vConstruct">
        ///     to make dict value
        /// </param>
        public KTable(
            string bootstrapServers, 
            string topic,
            int minPartition, int maxPartition, 
            IDeserializer<MK> keyDeserializer, 
            IDeserializer<MV> valueDeserializer,
            Func<MK, MV, K> kConstruct,
            Func<MK, MV, V> vConstruct)
        {
            var config = new Dictionary<string, object>
            {
                { "bootstrap.servers", bootstrapServers },
                { "api.version.request", true },
                { "group.id", Guid.NewGuid().ToString() }, // shouldn't matter.
                { "enable.auto.commit", false }
            };

            d = new Dictionary<K, V>();

            using (var consumer = new Consumer<MK, MV>(config, keyDeserializer, valueDeserializer))
            {
                int finishedCount = 0;
            
                consumer.OnMessage += (_, msg) => 
                {
                    var k = kConstruct(msg.Key, msg.Value);
                    var v = vConstruct(msg.Key, msg.Value);

                    if (d.ContainsKey(k))
                    {
                        d[k] = v;
                    }
                    else
                    {
                        d.Add(k, v);
                    }
                };

                consumer.OnPartitionEOF += (_, e) =>
                {
                    finishedCount += 1;
                };

                while (finishedCount != (maxPartition - minPartition + 1))
                {
                    consumer.Poll(100);
                }
            }
        }

        /// <summary>
        ///     val at k
        /// </summary>
        public V this[K key]
        {
            get
            {
                return d[key];
            }
        }

        /// <summary>
        ///     get enumerator
        /// </summary>
        /// <returns>
        ///     enumerator
        /// </returns>
        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<K, V>>)d).GetEnumerator();
        }

        /// <summary>
        ///     get enumerator
        /// </summary>
        /// <returns>
        ///     enumerator
        /// </returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<K, V>>)d).GetEnumerator();
        }
    }
}

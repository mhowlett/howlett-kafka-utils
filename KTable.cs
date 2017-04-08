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
    public class KTable<K, V> : IEnumerable<KeyValuePair<K, V>>
    {
        private KTable<K, V, K, V> d;

        /// <summary>
        ///     constructor
        /// </summary>
        /// <param name="bootstrapServers">
        ///     bs
        /// </param>
        /// <param name="topic">
        ///     t
        /// </param>
        /// <param name="minPartition">
        ///     mp
        /// </param>
        /// <param name="maxPartition">
        ///     mp
        /// </param>
        /// <param name="keyDeserializer">
        ///     k
        /// </param>
        /// <param name="valueDeserializer">
        ///     v
        /// </param>
        /// <param name="logProgressEvery">
        ///     p
        /// </param>
        /// <param name="limit">
        ///     p
        /// </param>
        public KTable(
            string bootstrapServers, 
            string topic,
            int minPartition, int maxPartition, 
            IDeserializer<K> keyDeserializer, 
            IDeserializer<V> valueDeserializer,
            int limit = -1,
            int logProgressEvery = 100000)
        {
            d = new KTable<K, V, K, V>(
                bootstrapServers,
                topic,
                minPartition, maxPartition,
                keyDeserializer,
                valueDeserializer,
                (k, v) => k, (k, v) => v,
                limit,
                logProgressEvery
            );
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
        ///     e
        /// </summary>
        /// <returns></returns>
        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<K, V>>)d).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<K, V>>)d).GetEnumerator();
        }

        /// <summary>
        ///     contains key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool ContainsKey(K key)
        {
            return d.ContainsKey(key);
        }
    }

    /// <summary>
    ///     K,V table materialized from a stream.
    /// </summary>
    public class KTable<MK, MV, K, V> : IEnumerable<KeyValuePair<K, V>>
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
        /// <param name="logProgressEvery">
        ///     -ve - don't log.
        /// </param>
        /// <param name="vConstruct">
        ///     to make dict value
        /// </param>
        /// <param name="limit">
        ///     max number to read.
        /// </param>
        public KTable(
            string bootstrapServers, 
            string topic,
            int minPartition, int maxPartition, 
            IDeserializer<MK> keyDeserializer, 
            IDeserializer<MV> valueDeserializer,
            Func<MK, MV, K> kConstruct,
            Func<MK, MV, V> vConstruct,
            int limit = -1,
            int logProgressEvery = 100000)
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
                int cnt = 0;

                consumer.OnMessage += (_, msg) => 
                {
                    if (logProgressEvery > 0)
                    {
                        if (++cnt % logProgressEvery == 0)
                        {
                            Console.WriteLine("... " + cnt);
                        }
                    }

                    var k = kConstruct(msg.Key, msg.Value);

                    if (k == null)
                    {
                        return;
                    }

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

                var ps = new List<TopicPartitionOffset>();
                for (var i=minPartition; i<=maxPartition; ++i)
                {
                    ps.Add(new TopicPartitionOffset(topic, i, 0));
                }
                
                consumer.Assign(ps);
                
                while (finishedCount != (maxPartition - minPartition + 1))
                {
                    consumer.Poll(100);

                    if (limit != -1 && cnt >= limit)
                    {
                        break;
                    }
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

        /// <summary>
        ///     contains key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool ContainsKey(K key)
        {
            return d.ContainsKey(key);
        }
    }
}

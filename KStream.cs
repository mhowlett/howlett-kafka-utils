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
        public class KStreamEnumerator<K, V> : IEnumerator<KeyValuePair<K, V>>
        {
            public KStreamEnumerator(KStream<K, V> stream)
            {

            }

            public KeyValuePair<K, V> Current => throw new NotImplementedException();

            object IEnumerator.Current => throw new NotImplementedException();

            public bool MoveNext()
            {
                throw new NotImplementedException();
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            #region IDisposable Support
            private bool disposedValue = false; // To detect redundant calls

            protected virtual void Dispose(bool disposing)
            {
                if (!disposedValue)
                {
                    if (disposing)
                    {
                        // TODO: dispose managed state (managed objects).
                    }

                    // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                    // TODO: set large fields to null.

                    disposedValue = true;
                }
            }

            // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
            // ~KStreamEnumerator() {
            //   // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            //   Dispose(false);
            // }

            // This code added to correctly implement the disposable pattern.
            public void Dispose()
            {
                // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
                Dispose(true);
                // TODO: uncomment the following line if the finalizer is overridden above.
                // GC.SuppressFinalize(this);
            }
            #endregion
        }

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
            return new KStreamEnumerator<K, V>(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }
}

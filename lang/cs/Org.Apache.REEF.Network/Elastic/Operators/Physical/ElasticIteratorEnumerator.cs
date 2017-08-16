using Org.Apache.REEF.Tang.Annotations;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    public abstract class ElasticIteratorEnumerator<T> : IEnumerator<T>
    {
        protected T State { get; set; }

        public abstract bool MoveNext();

        public void Reset()
        {
            throw new InvalidOperationException("Iterator operator does not support Reset");
        }

        public abstract T Current { get; }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        public void Dispose()
        {
        }
    }
}

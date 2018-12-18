using Org.Apache.REEF.Network.Elastic.Failures;
using Org.Apache.REEF.Network.Elastic.Failures.Enum;
using Org.Apache.REEF.Network.Elastic.Operators.Physical.Enum;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Org.Apache.REEF.Network.Elastic.Operators.Physical.Impl
{
    class EmptyOperator : IElasticOperator
    {
        public EmptyOperator()
        {
            OperatorName = Constants.Empty;
        }

        public string OperatorName { get; private set; }

        public int OperatorId
        {
            get { return -1; }
        }

        internal CheckpointLevel CheckpointLevel
        {
            get { return CheckpointLevel.None; }
        }

        public IElasticIterator IteratorReference { private get; set; }

        public CancellationTokenSource CancellationSource { get; set; }

        public Action OnTaskRescheduled { get; private set; }

        public string FailureInfo
        {
            get
            {
                string iteration = IteratorReference == null ? "-1" : IteratorReference.Current.ToString();
                return ((int)PositionTracker.Nil).ToString() + ":" + iteration;
            }
        }

        public void Dispose()
        {
        }

        public void ResetPosition()
        {
        }

        public void WaitCompletionBeforeDisposing()
        {
        }

        public void WaitForTaskRegistration(CancellationTokenSource cancellationSource = null)
        {
        }
    }
}

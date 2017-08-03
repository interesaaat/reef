using System;
using System.Threading;
using Org.Apache.REEF.Network.Group.Operators;
using Org.Apache.REEF.Tang.Annotations;
using Org.Apache.REEF.Network.Elastic.Config;
using System.Collections.Generic;
using Org.Apache.REEF.Tang.Formats;
using Org.Apache.REEF.Tang.Interface;
using Org.Apache.REEF.Tang.Util;
using Org.Apache.REEF.Network.Elastic.Operators.Physical;
using System.Linq;

namespace Org.Apache.REEF.Network.Elastic.Clients
{
    public class DefaultTaskSetSubscription : IElasticTaskSetSubscription
    {
        private readonly string _name;
        private readonly IDictionary<int, object> _operators;

        [Inject]
        private DefaultTaskSetSubscription(
           [Parameter(typeof(GroupCommunicationConfigurationOptions.SubscriptionName))] string subscriptionName,
           [Parameter(typeof(GroupCommunicationConfigurationOptions.SerializedOperatorConfigs))] ISet<string> operatorConfigs,
           AvroConfigurationSerializer configSerializer,
           IInjector injector)
        {
            _name = subscriptionName;

            _operators = new SortedDictionary<int, object>();

            foreach (string operatorConfigStr in operatorConfigs)
            {
                IConfiguration operatorConfig = configSerializer.FromString(operatorConfigStr);

                IInjector operatorInjector = injector.ForkInjector(operatorConfig);
                string msgType = operatorInjector.GetNamedInstance<OperatorsConfiguration.MessageType, string>(
                    GenericType<OperatorsConfiguration.MessageType>.Class);
                int id = operatorInjector.GetNamedInstance<OperatorsConfiguration.OperatorId, int>(
                    GenericType<OperatorsConfiguration.OperatorId>.Class);

                Type groupCommOperatorGenericInterface = typeof(IElasticOperator<>);
                Type groupCommOperatorInterface = groupCommOperatorGenericInterface.MakeGenericType(Type.GetType(msgType));
                var operatorObj = operatorInjector.GetInstance(groupCommOperatorInterface);

                _operators.Add(id, operatorObj);
            }
        }

        public string SubscriptionName
        {
            get { return _name; }
        }

        public void WaitingForRegistration(CancellationTokenSource cancellationSource)
        {
            foreach (var op in _operators.Values)
            {
                ((IRegistration)op).WaitingForRegistration(cancellationSource);
            }
        }

        public IBroadcastReceiver<T> GetBroadcastReceiver<T>(string operatorName)
        {
            throw new NotImplementedException();
        }

        public IBroadcastSender<T> GetBroadcastSender<T>(string operatorName)
        {
            throw new NotImplementedException();
        }

        public IReduceReceiver<T> GetReduceReceiver<T>(string operatorName)
        {
            throw new NotImplementedException();
        }

        public IReduceSender<T> GetReduceSender<T>(string operatorName)
        {
            throw new NotImplementedException();
        }

        public IScatterReceiver<T> GetScatterReceiver<T>(string operatorName)
        {
            throw new NotImplementedException();
        }

        public IScatterSender<T> GetScatterSender<T>(string operatorName)
        {
            throw new NotImplementedException();
        }

        public ITracker CreateTracker()
        {
            throw new NotImplementedException();
        }
    }
}
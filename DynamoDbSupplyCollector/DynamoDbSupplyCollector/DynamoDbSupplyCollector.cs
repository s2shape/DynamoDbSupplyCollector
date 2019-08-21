using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace DynamoDbSupplyCollector
{
    public class DynamoDbSupplyCollector : SupplyCollectorBase
    {
        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize)
        {
            throw new NotImplementedException();
        }

        public override List<string> DataStoreTypes()
        {
            return new List<string>() { "DynamoDB" };
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container)
        {
            throw new NotImplementedException();
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container)
        {
            throw new NotImplementedException();
        }

        public override bool TestConnection(DataContainer container)
        {
            throw new NotImplementedException();
        }
    }
}

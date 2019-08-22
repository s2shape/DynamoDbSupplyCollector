using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Newtonsoft.Json.Linq;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DynamoDbSupplyCollector
{
    internal static class DynamoAttributeExtensions
    {
        public static List<DataEntity> GetSchema(this Dictionary<string, AttributeValue> src)
        {
            //var json = JsonConvert.SerializeObject(src);

            var json2 = Document.FromAttributeMap(src).ToJson();

            var jObj = JObject.Parse(json2);



            throw new NotImplementedException();
        }

        public static bool HasValue(this Dictionary<string, AttributeValue> src)
        {
            return src.Values.Any();
        }

        public static string GetValue(this Dictionary<string, AttributeValue> src)
        {
            // DynamoDB attribute types https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
            // this is no any values when the actual value is undefined (null in .net terms)
            if (!src.HasValue())
                return null;

            var val = src.Values.First();

            if (!string.IsNullOrWhiteSpace(val.S))
                return val.S;

            if (!string.IsNullOrWhiteSpace(val.N))
                return val.N;

            if (val.NULL)
                return null;

            throw new NotSupportedException("CollectSample doesn't support complex values such as arrays, nested objects etc.");
        }

        public static bool IsSimpleValue(this Dictionary<string, AttributeValue> src)
        {
            // this is no any values when the actual value is undefined (null in .net terms)
            if (!src.HasValue())
                return true;

            var val = src.Values.First();

            if (!string.IsNullOrWhiteSpace(val.S) ||
                !string.IsNullOrWhiteSpace(val.N) ||
                val.NULL)
                return true;

            return false;
        }
    }
}

using Amazon.DynamoDBv2.Model;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DynamoDbSupplyCollector
{
    public static class DynamoAttributeExtensions
    {
        public static List<DataEntity> GetSchema(this Dictionary<string, AttributeValue> src,
            DataContainer dataContainer,
            DataCollection dataCollection)
        {
            var dataEntities = new List<DataEntity>();

            Traverse(src);

            return dataEntities.DistinctBy(e => e.Name).ToList();

            void Traverse(Dictionary<string, AttributeValue> current, string dataEntityName = null)
            {
                if (current.HasValue())
                {
                    if (current.IsSimpleValue())
                    {
                        var key = current.Keys.First();
                        var newEntityName = GetEntityName(dataEntityName, key);

                        var (v, t) = GetMeta(current);
                        dataEntities.Add(new DataEntity(newEntityName, t, v, dataContainer, dataCollection));
                    }
                    else if (current.Values.Count == 1)
                    {
                        var value = current.Values.First();
                        var key = current.Keys.First();
                        var newEntityName = GetEntityName(dataEntityName, key);

                        if (value.IsMSet)
                        {
                            Traverse(value.M, newEntityName);
                        }
                        else if (value.IsLSet)
                        {
                            var list = value.L;

                            foreach (var item in list)
                            {
                                Traverse(new Dictionary<string, AttributeValue>
                                {
                                    { "", item }
                                }, newEntityName);
                            }
                        }
                    }
                    else
                    {
                        for (int i = 0; i < current.Keys.Count; i++)
                        {
                            var key = current.Keys.ElementAt(i);
                            var value = current.Values.ElementAt(i);
                            var newEntityName = GetEntityName(dataEntityName, key);

                            Traverse(new Dictionary<string, AttributeValue>
                            {
                                { "", value }
                            }, newEntityName);
                        }
                    }
                }
            }
        }

        private static string GetEntityName(string dataEntityName, string key)
        {
            if (key == "")
                return dataEntityName;

            return string.IsNullOrEmpty(dataEntityName) ? key : $"{dataEntityName}.{key}";
        }

        public static bool HasValue(this Dictionary<string, AttributeValue> src)
        {
            return src.Values.Any();
        }

        public static (string, DataType) GetMeta(this Dictionary<string, AttributeValue> src)
        {
            // DynamoDB attribute types https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
            // this is no any values when the actual value is undefined (null in .net terms)
            if (!src.HasValue())
                return (null, DataType.Unknown);

            var val = src.Values.First();

            if (!string.IsNullOrWhiteSpace(val.S))
                return (val.S, DataType.String);

            if (!string.IsNullOrWhiteSpace(val.N))
                return (val.N, DataType.Decimal);

            if (val.NULL)
                return (null, DataType.Unknown);

            throw new NotSupportedException("CollectSample doesn't support complex values such as arrays, nested objects etc.");
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

            if (src.Values.Count > 1)
                return false;

            var val = src.Values.First();

            if (!string.IsNullOrWhiteSpace(val.S) ||
                !string.IsNullOrWhiteSpace(val.N) ||
                val.NULL)
                return true;

            return false;
        }
    }

    public static class LinqExtensions
    {
        public static IEnumerable<TSource> DistinctBy<TSource, TKey>
            (this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            HashSet<TKey> seenKeys = new HashSet<TKey>();
            foreach (TSource element in source)
            {
                if (seenKeys.Add(keySelector(element)))
                {
                    yield return element;
                }
            }
        }
    }
}

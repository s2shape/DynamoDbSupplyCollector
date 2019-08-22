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

                        var (dbType, dataType, _) = GetValue(current);
                        dataEntities.Add(new DataEntity(newEntityName, dataType, dbType, dataContainer, dataCollection));
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

        public static List<string> CollectSample(
            this Dictionary<string, AttributeValue> src,
            string dataEntityName)
        {
            if (src.IsSimpleValue())
            {
                var attr = src[dataEntityName];
                var (_, _, value) = attr.GetValue();

                return new List<string> { value };
            }
            else //if (IsNestedObject(dataEntityName))
            {
                var dataEntityPath = dataEntityName.Split(".");

                var rootPath = dataEntityPath.First();

                var root = src.Values.First();

                // path issue
                string subPath = string.Join('.', dataEntityPath.Skip(1));
                if (root.IsMSet)
                {
                    return CollectSample(root.M, subPath);
                }
                else if (root.IsLSet)
                {
                    var list = root.L;

                    foreach (var item in list)
                    {
                        return CollectSample(new Dictionary<string, AttributeValue> { { "", item } }, subPath);
                    }
                }
            }

            throw new NotImplementedException();
        }

        private static bool IsNestedObject(string entityName) => entityName.Contains(".");

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

        private static (string, DataType, string) GetValue(this Dictionary<string, AttributeValue> src)
        {
            // DynamoDB attribute types https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_AttributeValue.html
            if (!src.HasValue())
                return ("undefined", DataType.Unknown, null);

            var val = src.Values.First();

            return val.GetValue();
        }

        private static (string, DataType, string) GetValue(this AttributeValue src)
        {
            if (!string.IsNullOrWhiteSpace(src.S))
                return ("S", DataType.String, src.S);

            if (!string.IsNullOrWhiteSpace(src.N))
                return ("N", DataType.Decimal, src.N);

            if (src.IsBOOLSet)
                return ("BOOL", DataType.Boolean, src.BOOL ? "1" : "0");

            if (src.NS?.Any() ?? false)
                return ("NS", DataType.Unknown, null);

            if (src.SS?.Any() ?? false)
                return ("SS", DataType.Unknown, null);

            if (src.BS?.Any() ?? false)
                return ("BS", DataType.Unknown, null);

            if (src.B != null)
                return ("B", DataType.ByteArray, null);

            if (src.NULL)
                return ("NULL", DataType.Unknown, null);

            throw new NotSupportedException("Couldn't collect sample.");
        }

        private static bool IsSimpleValue(this Dictionary<string, AttributeValue> src)
        {
            // this is no any values when the actual value is undefined (null in .net terms)
            if (!src.HasValue())
                return true;

            if (src.Values.Count > 1)
                return false;

            var val = src.Values.First();

            if (!string.IsNullOrWhiteSpace(val.S) ||
                !string.IsNullOrWhiteSpace(val.N) ||
                val.NS.Any() ||
                val.SS.Any() ||
                val.IsBOOLSet ||
                val.BS.Any() ||
                val.B != null ||
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

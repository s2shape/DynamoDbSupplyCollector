using Amazon.DynamoDBv2.Model;
using S2.BlackSwan.SupplyCollector.Models;
using System;
using System.Collections.Generic;
using System.IO;
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

                            list.ForEach(x => Traverse(ToDictionary(x), newEntityName));
                        }
                    }
                    else
                    {
                        for (int i = 0; i < current.Keys.Count; i++)
                        {
                            var key = current.Keys.ElementAt(i);
                            var value = current.Values.ElementAt(i);
                            var newEntityName = GetEntityName(dataEntityName, key);

                            Traverse(ToDictionary(value), newEntityName);
                        }
                    }
                }
            }
        }

        public static List<string> CollectSample(
            this Dictionary<string, AttributeValue> src,
            string dataEntityName,
            List<string> initialSamples = null)
        {
            if (initialSamples == null)
                initialSamples = new List<string>();

            if (src.IsSimpleValue() && src.ContainsKey(dataEntityName))
            {
                var attr = src[dataEntityName];
                var (_, _, value) = attr.GetValue();

                initialSamples.AddRange(value);
            }
            else
            {
                // is key-value pairs of leaf level
                if (!IsNestedObject(dataEntityName) && src.Values.Count > 1 && src.ContainsKey(dataEntityName))
                {
                    var (_, _, value) = src[dataEntityName].GetValue();
                    initialSamples.AddRange(value);
                }
                else
                {
                    var dataEntityPath = dataEntityName.Split(".");
                    string subPath = string.Join('.', dataEntityPath.Skip(1));

                    if (src.ContainsKey(dataEntityPath.First()) || IsListItem(src))
                    {
                        var root = src.ContainsKey(dataEntityPath.First()) ?
                            src[dataEntityPath.First()] : src[""];

                        if (root.IsMSet)
                        {
                            CollectSample(root.M, subPath, initialSamples);
                        }
                        else if (root.IsLSet)
                        {
                            var list = root.L;
                            list.ForEach(x => CollectSample(ToDictionary(x), dataEntityName, initialSamples));
                        }
                    }
                }
            }

            return initialSamples;
        }

        private static bool IsListItem(Dictionary<string, AttributeValue> src)
        {
            return src.Keys.Count == 1 && src.Keys.First() == "";
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

        private static (string, DataType, List<string>) GetValue(this Dictionary<string, AttributeValue> src)
        {
            // DynamoDB attribute types https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_AttributeValue.html
            if (!src.HasValue())
                return ("undefined", DataType.Unknown, null);

            var val = src.Values.First();

            return val.GetValue();
        }

        private static (string, DataType, List<string>) GetValue(this AttributeValue src)
        {
            if (!string.IsNullOrWhiteSpace(src.S))
                return ("S", DataType.String, List(src.S));

            if (!string.IsNullOrWhiteSpace(src.N))
                return ("N", DataType.Decimal, List(src.N));

            if (src.IsBOOLSet)
                return ("BOOL", DataType.Boolean, List(src.BOOL ? "1" : "0"));

            if (src.NS?.Any() ?? false)
                return ("NS", DataType.Unknown, src.NS);

            if (src.SS?.Any() ?? false)
                return ("SS", DataType.Unknown, src.SS);

            if (src.BS?.Any() ?? false)
                return ("BS", DataType.Unknown, src.BS.Select(FromBinary).ToList());

            if (src.B != null)
                return ("B", DataType.ByteArray, List(FromBinary(src.B)));

            if (src.NULL)
                return ("NULL", DataType.Unknown, null);

            throw new NotSupportedException("Couldn't collect sample.");
        }

        private static string FromBinary(MemoryStream src)
        {
            // Due to the documentation binary data is Base64-encoded. https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_AttributeValue.html
            return Convert.ToBase64String(src.ToArray());
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

        private static Dictionary<string, AttributeValue> ToDictionary(AttributeValue val, string key = "") =>
            new Dictionary<string, AttributeValue> { { key, val } };

        private static List<T> List<T>(T item) =>
            new List<T> { item };
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

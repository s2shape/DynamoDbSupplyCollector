using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DynamoDbSupplyCollector
{
    internal static class DynamoAttributeExtensions
    {
        public static bool HasValue(this Dictionary<string, AttributeValue> attr)
        {
            return attr.Values.Any();
        }

        public static string GetValue(this Dictionary<string, AttributeValue> attr)
        {
            // DynamoDB attribute types https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
            // this is no any values when the actual value is undefined (null in .net terms)
            if (!attr.HasValue())
                return null;

            var val = attr.Values.First();

            if (!string.IsNullOrWhiteSpace(val.S))
                return val.S;

            if (!string.IsNullOrWhiteSpace(val.N))
                return val.N;

            if (val.NULL)
                return null;

            throw new NotSupportedException("CollectSample doesn't support complex values such as arrays, nested objects etc.");
        }

        public static bool IsSimpleValue(this Dictionary<string, AttributeValue> attr)
        {
            // this is no any values when the actual value is undefined (null in .net terms)
            if (!attr.HasValue())
                return true;

            var val = attr.Values.First();

            if (!string.IsNullOrWhiteSpace(val.S) ||
                !string.IsNullOrWhiteSpace(val.N) ||
                val.NULL)
                return true;

            return false;
        }
    }
}

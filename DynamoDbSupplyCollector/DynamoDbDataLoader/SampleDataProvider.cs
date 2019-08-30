using DynamoDbDataLoader.Models;
using System;
using System.Collections.Generic;

namespace DynamoDbDataLoader
{
    public class SampleDataProvider
    {
        public List<Person> GetPeople(int number)
        {
            var list = new List<Person>(number);

            var noLastName = new Person() { Id = Guid.NewGuid(), FirstName = "Eugene" };
            var deleted = new Person() { Id = Guid.NewGuid(), FirstName = "Eugene (deleted)", IsDeleted = true };
            var noType1 = new Person(1000000, 10, 10);
            noType1.Addresses["type1"] = null;

            list.Add(noType1);
            list.Add(noLastName);
            list.Add(deleted);

            for (int i = 0; i < number; i++)
            {
                var person = new Person(i, 2, 3);
                list.Add(person);
            }

            return list;
        }
    }
}

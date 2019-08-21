using CsvHelper;
using DynamoDbDataLoader.Models;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DynamoDbDataLoader
{
    public class SampleDataProvider
    {
        // TODO: refactor this
        //string rootPath = "../../../SampleData";

        public List<Person> GetPeople(int number)
        {
            var list = new List<Person>(number);

            for (int i = 0; i < number; i++)
            {
                var person = new Person(i, 2, 3);
                list.Add(person);
            }

            return list;
        }

        //public List<ContactsAudit> GetContactsAudit()
        //{
        //    return new List<ContactsAudit>
        //    {
        //        new ContactsAudit
        //        {
        //            AUDIT_ID = Guid.NewGuid(),
        //            ASSIGNED_USER_ID = Guid.NewGuid(),
        //            ASSISTANT = "casdfsadfs"
        //        },
        //        new ContactsAudit
        //        {
        //            AUDIT_ID = Guid.NewGuid(),
        //            ASSIGNED_USER_ID = Guid.NewGuid(),
        //            ASSISTANT = "casdfsadfs"
        //        },
        //        new ContactsAudit
        //        {
        //            AUDIT_ID = Guid.NewGuid(),
        //            ASSIGNED_USER_ID = Guid.NewGuid(),
        //            ASSISTANT = "casdfsadfs"
        //        }
        //    };

        //    return LoadFile<ContactsAudit>($"{rootPath}\\CONTACTS_AUDIT.CSV");
        //}

        //public List<ContactsAudit> GetEmails()
        //{
        //    return LoadFile<ContactsAudit>($"{rootPath}\\EMAILS.CSV");
        //}

        //public List<ContactsAudit> GetLeads()
        //{
        //    return LoadFile<ContactsAudit>($"{rootPath}\\LEADS.CSV");
        //}

        //private List<T> LoadFile<T>(string path)
        //{
        //    using (var streamReader = new StreamReader(path))
        //    {
        //        using (var reader = new CsvReader(streamReader))
        //        {
        //            reader.Configuration.MissingFieldFound = null;
        //            reader.Configuration.RegisterClassMap<ContactsAuditMap>();

        //            reader.Read();
        //            reader.ReadHeader();
        //            var header = reader.Context.HeaderRecord;

        //            return reader.GetRecords<T>().ToList();
        //        }
        //    }
        //}
    }
}

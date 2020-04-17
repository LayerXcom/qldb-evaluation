using System;
using System.Collections.Generic;
using System.Linq;
using Amazon.QLDB;
using Amazon.QLDB.Driver;
using Amazon.QLDBSession.Model;
using Amazon.IonDotnet;
using Amazon.IonDotnet.Tree;

namespace QuantumVerification
{
    public class Person
    {
        public string FirstName { get; }
        public string LastName { get; }
        public long CreatedAt { get; }

        public Person(string firstName, string lastName)
        {
            FirstName = firstName;
            LastName = lastName;
            CreatedAt = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        }

        public Person(IIonValue ionValue)
        {
            FirstName = ionValue.GetField("FirstName").StringValue;
            LastName = ionValue.GetField("LastName").StringValue;
            CreatedAt = ionValue.GetField("CreatedAt").LongValue;
        }

        public string ToJson() => 
            string.Format("{{ 'FirstName': '{0}',  'LastName': '{1}', 'CreatedAt': {2} }}", FirstName, LastName, CreatedAt);
    }
    
    class Program
    {
        static void Main(string[] args)
        {
            var builder = PooledQldbDriver.Builder();
            var driver = builder.WithLedger("demo-ledger-1").Build();
            var session = driver.GetSession();
            var personForCreate = new Person("akinama", "taro");
            session.Execute($"INSERT INTO Person << {personForCreate.ToJson()} >>;");
            var ion = session.Execute("SELECT * FROM Person") as BufferedResult;
            var people = new List<Person>();
            foreach (var ionValue in ion)
            {
                var person = new Person(ionValue);
                people.Add(person);
                Console.WriteLine($"FirstName: {person.FirstName}, LastName: {person.LastName}, CreatedAt: {person.CreatedAt}");
            }
        }
    }
}

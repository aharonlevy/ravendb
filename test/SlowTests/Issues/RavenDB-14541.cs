

using System;
using System.Linq;
using Confluent.Kafka;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_14541: RavenTestBase
{
    public RavenDB_14541(ITestOutputHelper output) : base(output)
    {
    }
    private class Address
    {
        public string CountryState { get; set; }
        public string City { get; set; }
    }

    private class State
    {
        public string Name { get; set; }
    }

    private class QueryResult
    {
        public string Comapny { get; set; }
    }

    [Fact]

    public void ShouldThrowNotSupportedException()
    {
        try
        {
            using (DocumentStore store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Company = "1", Employee = "employees/1" }, "orders/1-A");
                    session.Store(new Order { Company = "2", Employee = "employees/2" }, "orders/2-A");


                    session.Store(new Employee { FirstName = "a" }, "employees/1");
                    session.Store(new Employee { FirstName = "b" }, "employees/2");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {

                    var query3 = from u in session.Query<Order>()
                        let includes = RavenQuery.Include<Order>(u.Employee)
                        select new QueryResult
                        {
                            Comapny = u.Company
                        };
                }
            }
        }
        catch (Exception e)
        {
            Assert.NotNull(e);
            Assert.Contains("You can't use the include that way try: let _= RavenQuery.Include<T>()", e.InnerException.Message);
        }
        

    }


    


    [Fact]

    public void SessionQuerySelectAdressFromIncludeDoc_UsingRavenQueryCounter()
    {
        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Address { CountryState = "states/1#zip07", City = "new-york" });
                session.Store(new Address { CountryState = "states/2#zip05", City = "haifa" });

                session.Store(new State { Name = "Alabama" }, "states/1");
                session.Store(new State { Name = "Minassota" }, "states/2");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query3 = from a in session.Query<Address>()
                             let _ = RavenQuery.Include<State>(a.CountryState.Split('#',StringSplitOptions.None)[0])
                             select new
                             {
                                 Name= _.Name
                             };
                var res1 = query3.ToString();
                var res2 = query3.ToList();
                Assert.Equal(2, res2.Count);
                Assert.Equal(null, res2[0].Name);
                Assert.Equal(null, res2[1].Name);

                var doc1 = session.Load<State>("states/1");
                var doc2 = session.Load<State>("states/2");

                Assert.Equal(1, session.Advanced.NumberOfRequests);

            }
        }
    }



}


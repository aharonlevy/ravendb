

using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using FastTests;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_14541 : RavenTestBase
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

    private class User
    {
        public string UserName { get; set; }
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
                        select new QueryResult { Comapny = u.Company };
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
                    let _ = RavenQuery.Include<State>(a.CountryState.Split('#', StringSplitOptions.None)[0])
                    select new { Name = _.Name };
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

    [Fact]
    public void SessionQuerySelectTimeSeriesFromIncludeDoc_UsingRavenQuery()
    {
        var baseline = DateTime.Today;

        using (DocumentStore store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new User { UserName = "nur"}, "user/nur");
                session.Store(new User { UserName = "arik"}, "user/arik");

                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(2),5 , "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(3), 12, "views/nur");
                session.TimeSeriesFor("user/nur", "CurViews").Append(baseline.AddHours(4), 15, "views/nur");

                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(1), 3, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(2), 19, "views/arik");
                session.TimeSeriesFor("user/arik", "CurViews").Append(baseline.AddHours(3), 80, "views/arik");

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var start = baseline;
                var end = baseline.AddHours(5);

                /*var order = session.Query<Core.Utils.Entities.User>()
                    .Include(i => i
                        .IncludeTimeSeries("CurViews"));
                var OctetString = order.ToString();
                var Olist = order.ToList();*/

                /*var q1 = session.Advanced.RawQuery<dynamic>(@"
declare function output(a, $p0, $p1) {
	var _ = timeseries2('Heartrate', $p0, $p1)
	return { Name : a.UserName };
}
from 'Users' as a select output(a, '2023-02-19T00:03:00.0000000Z', '2023-02-19T00:05:00.0000000Z')

");
                var str = q1.ToString();
                var res = q1.ToList();*/


                var query = from a in session.Query<User>()
                    let _ = RavenQuery.IncludeTimeSeries<User>("CurViews", start, end)
                   // TODO: add test: let __ = RavenQuery.IncludeTimeSeries<User>("CurViews2", start, end)
                    select new {Name = a.UserName};



                var AsString = query.ToString();
                /*WaitForUserToContinueTheTest(store);*/
                var result = query.ToList();
                var numberOfReq = session.Advanced.NumberOfRequests;
                
                
                IEnumerable<TimeSeriesEntry> val = session.TimeSeriesFor("user/nur", "CurViews")
                    .Get(start, end);
                var newNumOfReq = session.Advanced.NumberOfRequests;
                Console.WriteLine("old req number: " + numberOfReq);
                Console.WriteLine("new req number: " + newNumOfReq);
                
                //   IEnumerable<TimeSeriesEntry> val = session.TimeSeriesFor(result[0], "CurViews")
                //       .Get(start, end);
            }
        }
    }
}


//-----------------------------------------------------------------------
// <copyright file="AccurateCount.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using FastTests;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Bugs
{
    public class AccurateCount : RavenTestBase
    {
        [Fact] 
        public void QueryableCountIsAccurate()
        {
            using(var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new[] {
                                                new IndexDefinition
                                                {
                                                    Maps = { "from user in docs.Users select new { user.Name }"}
                                                    ,
                                                    Name = "Users"
                                                }}));

                using(var s = store.OpenSession())
                {
                    for (int i = 0; i < 5; i++)
                    {
                        var clone = new User{Name = "Ayende #"+i};
                        s.Store(clone);
                    }
                    s.SaveChanges();
                }

                // wait for index
                using (var s = store.OpenSession())
                {
                    var count = s.Query<User>("Users")
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Count();
                    Assert.Equal(5, count);
                }

                using (var s = store.OpenSession())
                {
                    var queryable = s.Query<User>("Users");
                    Assert.Equal(queryable.ToArray().Length, queryable.Count());
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}

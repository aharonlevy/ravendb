﻿// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3193.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4645 : RavenTestBase
    {
        public RavenDB_4645(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldStopPullingTaskWhenSubscriptionIsDeleted()
        {
            using (var store = GetDocumentStore())
            {
                // insert few documents and fetch them using subscription
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from Companies"
                };
                var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch = 5,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var docs = new List<dynamic>();
                GC.KeepAlive(subscription.Run(batch => docs.AddRange(batch.Items.Select(item => item.Result))));
                Assert.True(SpinWait.SpinUntil(() => docs.Count == 10, TimeSpan.FromSeconds(60)));

                // all documents were fetched - time to delete subscription
                await store.Subscriptions.DeleteAsync(id);

                // verify if we don't get new items
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                // wait 3 seconds for new documents - we shouldn't get any
                Assert.False(SpinWait.SpinUntil(() => docs.Count != 10, TimeSpan.FromSeconds(3)));
            }
        }
    }
}

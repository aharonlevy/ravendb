﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Amazon.S3.Util;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using XunitLogger;

namespace SlowTests.Issues;

public class RavenDB_20084 : ClusterTestBase
{
    public RavenDB_20084(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ShouldUpdateBackupInfoIfDatabaseUnloaded()
    {
        const int clusterSize = 3;
        const int backupIntervalInMinutes = 1;

        var backupPath = NewDataPath(suffix: "BackupFolder");
        var databaseName = GetDatabaseName();

        var (_, leaderServer) = await CreateRaftCluster(numberOfNodes: clusterSize, shouldRunInMemory: false,
            customSettings: new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "1"
            });

        await CreateDatabaseInCluster(databaseName, clusterSize, leaderServer.WebUrl);

        using (var leaderStore = new DocumentStore
               {
                   Urls = new[] { leaderServer.WebUrl }, Conventions = new DocumentConventions { DisableTopologyUpdates = true }, Database = databaseName
               })
        {
            leaderStore.Initialize();

            // Populating the database and forcibly transitioning it to an idle state
            await Backup.FillClusterDatabaseWithRandomDataAsync(databaseSizeInMb: 1, leaderStore, clusterSize);

            var config = Backup.CreateBackupConfiguration(backupPath, fullBackupFrequency: "0 * * * *", incrementalBackupFrequency: $"*/{backupIntervalInMinutes} * * * *", mentorNode: leaderServer.ServerStore.NodeTag);
            var taskId = await Backup.UpdateConfigAndRunBackupAsync(leaderServer, config, leaderStore);

            var onGoingTaskBackup = await leaderStore.Maintenance.SendAsync(new GetOngoingTaskInfoOperation(taskId, OngoingTaskType.Backup)) as OngoingTaskBackup;
            Assert.True(onGoingTaskBackup is { LastFullBackup: { } });
            var expectedTime = onGoingTaskBackup.NextBackup.DateTime;
            
            leaderServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().ShouldFetchIdleStateImmediately = true;
            leaderServer.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = true;
            
            using (leaderServer.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverStoreContext))
            using (serverStoreContext.OpenReadTransaction())
            {
                // Ensuring the database is in an idle state
                WaitForValue( () => leaderServer.ServerStore.IdleDatabases.Count, 
                    expectedVal: 1,
                    timeout: Convert.ToInt32(TimeSpan.FromMinutes(5).TotalMilliseconds),
                    interval: Convert.ToInt32(TimeSpan.FromSeconds(1).TotalMilliseconds));
                Assert.Equal(1, leaderServer.ServerStore.IdleDatabases.Count);

                // No longer forcing the idle state for the database
                leaderServer.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().ShouldFetchIdleStateImmediately = false;
                leaderServer.ServerStore.DatabasesLandlord.SkipShouldContinueDisposeCheck = false;


                // Awaiting the next backup event to verify that the '/database' endpoint provides an updated value for the last incremental backup timestamp
                var client = leaderStore.GetRequestExecutor().HttpClient;
                DateTime lastBackupTime = default;
                await WaitForValueAsync(async () =>
                    {
                        var response = await client.GetAsync($"{leaderServer.WebUrl}/databases");
                        string result = response.Content.ReadAsStringAsync().Result;

                        var databaseResponse = serverStoreContext.Sync.ReadForMemory(result, "Databases");

                        if (databaseResponse.TryGet("Databases", out BlittableJsonReaderArray array) == false ||
                            ((BlittableJsonReaderObject)array[0]).TryGet("BackupInfo", out BlittableJsonReaderObject backupInfo) == false ||
                            backupInfo == null ||
                            backupInfo.TryGet("LastBackup", out lastBackupTime) == false)
                            return false;

                        return lastBackupTime == expectedTime;
                    },
                    expectedVal: true,
                    timeout: Convert.ToInt32(TimeSpan.FromMinutes(3).TotalMilliseconds),
                    interval: Convert.ToInt32(TimeSpan.FromSeconds(1).TotalMilliseconds));
                
                Assert.Equal(expectedTime, lastBackupTime);
                Assert.Equal(1, leaderServer.ServerStore.IdleDatabases.Count);

                // Verifying that the cluster storage contains the same value for consistency
                var operation = new GetPeriodicBackupStatusOperation(taskId);
                var backupStatus = (await leaderStore.Maintenance.SendAsync(operation)).Status;
                Assert.Equal(expectedTime, backupStatus.LastIncrementalBackupInternal);
                Assert.Equal(expectedTime, backupStatus.LastIncrementalBackup);
            }
        }
    }
}

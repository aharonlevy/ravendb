﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin
{
    public class AdminIndexHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/indexes", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task Put()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var createdIndexes = new List<KeyValuePair<string, long>>();
                var input = await context.ReadForMemoryAsync(RequestBodyStream(), "Indexes");
                if (input.TryGet("Indexes", out BlittableJsonReaderArray indexes) == false)
                    ThrowRequiredPropertyNameInRequest("Indexes");

                foreach (var indexToAdd in indexes)
                {
                    var indexDefinition = JsonDeserializationServer.IndexDefinition((BlittableJsonReaderObject)indexToAdd);

                    if (indexDefinition.Maps == null || indexDefinition.Maps.Count == 0)
                        throw new ArgumentException("Index must have a 'Maps' fields");
                    var etag = await Database.IndexStore.CreateIndex(indexDefinition);
                    createdIndexes.Add(new KeyValuePair<string, long>(indexDefinition.Name, etag));
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WriteArray(context, "Results", createdIndexes, (w, c, index) =>
                    {
                        w.WriteStartObject();
                        w.WritePropertyName(nameof(PutIndexResult.IndexId));
                        w.WriteInteger(index.Value);

                        w.WriteComma();

                        w.WritePropertyName(nameof(PutIndexResult.Index));
                        w.WriteString(index.Key);
                        w.WriteEndObject();
                    });

                    writer.WriteEndObject();
                }
            }
        }
        
        [RavenAction("/databases/*/admin/indexes/stop", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task Stop()
        {
            var types = HttpContext.Request.Query["type"];
            var names = HttpContext.Request.Query["name"];
            if (types.Count == 0 && names.Count == 0)
            {
                Database.IndexStore.StopIndexing();
                return NoContent();
            }

            if (types.Count != 0 && names.Count != 0)
                throw new ArgumentException("Query string value 'type' and 'names' are mutually exclusive.");

            if (types.Count != 0)
            {
                if (types.Count != 1)
                    throw new ArgumentException("Query string value 'type' must appear exactly once");
                if (string.IsNullOrWhiteSpace(types[0]))
                    throw new ArgumentException("Query string value 'type' must have a non empty value");

                if (string.Equals(types[0], "map", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StopMapIndexes();
                }
                else if (string.Equals(types[0], "map-reduce", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StopMapReduceIndexes();
                }
                else
                {
                    throw new ArgumentException("Query string value 'type' can only be 'map' or 'map-reduce' but was " + types[0]);
                }
            }
            else if (names.Count != 0)
            {
                if (names.Count != 1)
                    throw new ArgumentException("Query string value 'name' must appear exactly once");
                if (string.IsNullOrWhiteSpace(names[0]))
                    throw new ArgumentException("Query string value 'name' must have a non empty value");

                Database.IndexStore.StopIndex(names[0]);
            }

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/start", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task Start()
        {
            var types = HttpContext.Request.Query["type"];
            var names = HttpContext.Request.Query["name"];
            if (types.Count == 0 && names.Count == 0)
            {
                Database.IndexStore.StartIndexing();

                return NoContent();
            }

            if (types.Count != 0 && names.Count != 0)
                throw new ArgumentException("Query string value 'type' and 'names' are mutually exclusive.");

            if (types.Count != 0)
            {
                if (types.Count != 1)
                    throw new ArgumentException("Query string value 'type' must appear exactly once");
                if (string.IsNullOrWhiteSpace(types[0]))
                    throw new ArgumentException("Query string value 'type' must have a non empty value");

                if (string.Equals(types[0], "map", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StartMapIndexes();
                }
                else if (string.Equals(types[0], "map-reduce", StringComparison.OrdinalIgnoreCase))
                {
                    Database.IndexStore.StartMapReduceIndexes();
                }
            }
            else if (names.Count != 0)
            {
                if (names.Count != 1)
                    throw new ArgumentException("Query string value 'name' must appear exactly once");
                if (string.IsNullOrWhiteSpace(names[0]))
                    throw new ArgumentException("Query string value 'name' must have a non empty value");

                Database.IndexStore.StartIndex(names[0]);
            }

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/enable", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task Enable()
        {
            var name = GetStringQueryString("name");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Enable();

            return NoContent();
        }

        [RavenAction("/databases/*/admin/indexes/disable", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task Disable()
        {
            var name = GetStringQueryString("name");
            var index = Database.IndexStore.GetIndex(name);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(name);

            index.Disable();

            return NoContent();
        }
    }
}

using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Blockly.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Collections.Generic;

namespace Blockly.Blocks
{
    public static class BlockFunctions
    {
        private static CloudTableClient tableClient;
        private static CloudBlobClient blobClient;

        private static CloudTable modelTable;

        private static CloudBlobContainer librariesBlobContainer;
        private static CloudBlobContainer definitionsBlobContainer;
        private static CloudBlobContainer toolboxesBlobContainer;

        static BlockFunctions()
        {
            var cloudStorageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");

            tableClient = cloudStorageAccount.CreateCloudTableClient();

            modelTable = GetCloudTable("models");

            blobClient = cloudStorageAccount.CreateCloudBlobClient();

            librariesBlobContainer = GetBlobContainer("libraries");
            definitionsBlobContainer = GetBlobContainer("definitions");
            toolboxesBlobContainer = GetBlobContainer("toolboxes");
        }

        private static CloudBlobContainer GetBlobContainer(string containerName)
        {
            var blobContainer = blobClient.GetContainerReference(containerName);

            if (!blobContainer.ExistsAsync().GetAwaiter().GetResult())
                blobContainer.CreateAsync().GetAwaiter().GetResult();

            return blobContainer;
        }

        private static CloudTable GetCloudTable(string tableName)
        {
            var table = tableClient.GetTableReference(tableName);

            if (!table.ExistsAsync().GetAwaiter().GetResult())
                table.CreateAsync().GetAwaiter().GetResult();

            return table;
        }

        private static async Task<ModelDescriptor> GetModelDescriptorAsync(string id)
        {
            var retrieveOperation = TableOperation.Retrieve<DynamicTableEntity>("model", id);
            var tableResult = await modelTable.ExecuteAsync(retrieveOperation);
            var dynamicTableEntity = tableResult.Result as DynamicTableEntity;

            if (dynamicTableEntity is not null)
                return new ModelDescriptor()
                {
                    Name = dynamicTableEntity.Properties.ContainsKey("Name") ? dynamicTableEntity.Properties["Name"].StringValue : string.Empty,
                    DefinitionsBlobId = dynamicTableEntity.Properties.ContainsKey("DefinitionsBlobId") ? dynamicTableEntity.Properties["DefinitionsBlobId"].StringValue : string.Empty,
                    LibraryBlobId = dynamicTableEntity.Properties.ContainsKey("LibraryBlobId") ? dynamicTableEntity.Properties["LibraryBlobId"].StringValue : string.Empty,
                    ToolBoxBlobId = dynamicTableEntity.Properties.ContainsKey("ToolBoxBlobId") ? dynamicTableEntity.Properties["ToolBoxBlobId"].StringValue : string.Empty,
                };

            return null;
        }

        private static async Task<IEnumerable<Model>> GetAllModelsAsync()
        {
            var modelDescriptors = new List<Model>();

            TableContinuationToken token = null;

            do
            {
                var query = new TableQuery<DynamicTableEntity>();

                var queryResult = await modelTable.ExecuteQuerySegmentedAsync(query, token);

                foreach (var dynamicTableEntity in queryResult.Results)
                    modelDescriptors.Add(new Model()
                    {
                        Id = dynamicTableEntity.PartitionKey,
                        Name = dynamicTableEntity.Properties.ContainsKey("Name") ? dynamicTableEntity.Properties["Name"].StringValue : string.Empty,
                    });

                token = queryResult.ContinuationToken;

            } while (token != null);

            return modelDescriptors;
        }

        private static async Task<string> AddModelDescriptorAsync(ModelDescriptor modelDescriptor)
        {
            var id = Guid.NewGuid().ToString();

            var dynamicTableEntity = new DynamicTableEntity("model", id);
            dynamicTableEntity.Properties = new Dictionary<string, EntityProperty>();

            dynamicTableEntity.Properties.Add("Name", new EntityProperty(modelDescriptor.Name));
            dynamicTableEntity.Properties.Add("DefinitionsBlobId", new EntityProperty(modelDescriptor.DefinitionsBlobId));
            dynamicTableEntity.Properties.Add("LibraryBlobId", new EntityProperty(modelDescriptor.LibraryBlobId));
            dynamicTableEntity.Properties.Add("ToolBoxBlobId", new EntityProperty(modelDescriptor.ToolBoxBlobId));

            var insertOperation = TableOperation.Insert(dynamicTableEntity);

            await modelTable.ExecuteAsync(insertOperation);

            return id;
        }

        private static async Task UpdateModelDescriptorAsync(ModelDescriptor modelDescriptor, string id)
        {
            var currentModelDescriptor = await GetModelDescriptorAsync(id);

            if (currentModelDescriptor is not null)
            {
                var dynamicTableEntity = new DynamicTableEntity("model", id);
                dynamicTableEntity.Properties = new Dictionary<string, EntityProperty>();

                dynamicTableEntity.Properties.Add("Name", new EntityProperty(modelDescriptor.Name));
                dynamicTableEntity.Properties.Add("DefinitionsBlobId", new EntityProperty(modelDescriptor.DefinitionsBlobId));
                dynamicTableEntity.Properties.Add("LibraryBlobId", new EntityProperty(modelDescriptor.LibraryBlobId));
                dynamicTableEntity.Properties.Add("ToolBoxBlobId", new EntityProperty(modelDescriptor.ToolBoxBlobId));

                var replaceOpration = TableOperation.InsertOrMerge(dynamicTableEntity);

                await modelTable.ExecuteAsync(replaceOpration);
            }
        }

        private static async Task GetAsync(this CloudBlobContainer @this, string id, Stream stream)
        {
            var blockBlob = @this.GetBlockBlobReference(id);
            await blockBlob.DownloadToStreamAsync(stream);
        }

        private static async Task<string> AddBlobFromStreamAsync(this CloudBlobContainer @this, Stream stream)
        {
            var id = Guid.NewGuid().ToString();

            var blockBlob = @this.GetBlockBlobReference(id);
            await blockBlob.UploadFromStreamAsync(stream);

            return id;
        }

        private static async Task RemoveBlobAsync(this CloudBlobContainer @this, string id)
        {
            var blockBlob = @this.GetBlockBlobReference(id);
            await blockBlob.DeleteAsync();
        }


        [FunctionName("Model_Post")]
        public static async Task<IActionResult> ModelPost(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "{modelName}")] HttpRequest req,
            string modelName,
            ILogger log)
        {
            var modelDescriptor = new ModelDescriptor() { Name = modelName };

            var modelDescriptorId = await AddModelDescriptorAsync(modelDescriptor);

            return new OkObjectResult(new Model() { Id = modelDescriptorId, Name = modelName });
        }

        [FunctionName("Models_Get")]
        public static async Task<IActionResult> ModelsGet(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "models")] HttpRequest req,
            ILogger log)
        {
            return new ObjectResult(await GetAllModelsAsync());
        }

        [FunctionName("Libraries_Post")]
        public static async Task<IActionResult> LibrariesPost(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "libraries/{modelId}")] HttpRequest req,
            string modelId,
            ILogger log)
        {
            var modelDescriptor = await GetModelDescriptorAsync(modelId);

            if (modelDescriptor is not null)
            {
                if (!string.IsNullOrWhiteSpace(modelDescriptor.LibraryBlobId))
                    await librariesBlobContainer.RemoveBlobAsync(modelDescriptor.LibraryBlobId);

                modelDescriptor.LibraryBlobId = await librariesBlobContainer.AddBlobFromStreamAsync(req.Body);

                await UpdateModelDescriptorAsync(modelDescriptor, modelId);

                return new OkResult();
            }

            log.LogError($"Model not founded: {modelId}");

            return new NotFoundResult();
        }

        [FunctionName("Libraries_Get")]
        public static async Task<IActionResult> LibrariesGet(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "libraries/{modelId}")] HttpRequest req,
            string modelId,
            ILogger log)
        {
            var modelDescriptor = await GetModelDescriptorAsync(modelId);

            if (modelDescriptor is not null && !string.IsNullOrWhiteSpace(modelDescriptor.LibraryBlobId))
            {
                var stream = new MemoryStream();

                await librariesBlobContainer.GetAsync(modelDescriptor.LibraryBlobId, stream);
                stream.Position = 0;

                var contentResult = new ContentResult();
                contentResult.StatusCode = 200;
                contentResult.ContentType = "application/xml";
                contentResult.Content = await new StreamReader(stream).ReadToEndAsync();

                return contentResult;
            }

            log.LogError($"Model not founded: {modelId}");

            return new NotFoundResult();
        }

        [FunctionName("Definitions_Post")]
        public static async Task<IActionResult> DefinitionsPost(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "definitions/{modelId}")] HttpRequest req,
            string modelId,
            ILogger log)
        {
            var modelDescriptor = await GetModelDescriptorAsync(modelId);

            if (modelDescriptor is not null)
            {
                if (!string.IsNullOrWhiteSpace(modelDescriptor.DefinitionsBlobId))
                    await definitionsBlobContainer.RemoveBlobAsync(modelDescriptor.DefinitionsBlobId);

                modelDescriptor.DefinitionsBlobId = await definitionsBlobContainer.AddBlobFromStreamAsync(req.Body);

                await UpdateModelDescriptorAsync(modelDescriptor, modelId);

                return new OkResult();
            }

            log.LogError($"Model not founded: {modelId}");

            return new NotFoundResult();
        }

        [FunctionName("Definitions_Get")]
        public static async Task<IActionResult> DefinitionsGet(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "definitions/{modelId}")] HttpRequest req,
            string modelId,
            ILogger log)
        {
            var modelDescriptor = await GetModelDescriptorAsync(modelId);

            if (modelDescriptor is not null && !string.IsNullOrWhiteSpace(modelDescriptor.DefinitionsBlobId))
            {
                var stream = new MemoryStream();

                await definitionsBlobContainer.GetAsync(modelDescriptor.DefinitionsBlobId, stream);
                stream.Position = 0;

                var contentResult = new ContentResult();
                contentResult.StatusCode = 200;
                contentResult.ContentType = "application/json";
                contentResult.Content = await new StreamReader(stream).ReadToEndAsync();

                return contentResult;
            }

            log.LogError($"Model not founded: {modelId}");

            return new NotFoundResult();
        }

        [FunctionName("Toolboxes_Post")]
        public static async Task<IActionResult> toolboxesPost(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "toolboxes/{modelId}")] HttpRequest req,
            string modelId,
            ILogger log)
        {
            var modelDescriptor = await GetModelDescriptorAsync(modelId);

            if (modelDescriptor is not null)
            {
                if (!string.IsNullOrWhiteSpace(modelDescriptor.ToolBoxBlobId))
                    await toolboxesBlobContainer.RemoveBlobAsync(modelDescriptor.ToolBoxBlobId);

                modelDescriptor.ToolBoxBlobId = await toolboxesBlobContainer.AddBlobFromStreamAsync(req.Body);

                await UpdateModelDescriptorAsync(modelDescriptor, modelId);

                return new OkResult();
            }

            log.LogError($"Model not founded: {modelId}");

            return new NotFoundResult();
        }

        [FunctionName("Toolboxes_Get")]
        public static async Task<IActionResult> toolboxesGet(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "toolboxes/{modelId}")] HttpRequest req,
            string modelId,
            ILogger log)
        {
            var modelDescriptor = await GetModelDescriptorAsync(modelId);

            if (modelDescriptor is not null && !string.IsNullOrWhiteSpace(modelDescriptor.ToolBoxBlobId))
            {
                var stream = new MemoryStream();

                await toolboxesBlobContainer.GetAsync(modelDescriptor.ToolBoxBlobId, stream);
                stream.Position = 0;

                var contentResult = new ContentResult();
                contentResult.StatusCode = 200;
                contentResult.ContentType = "application/xml";
                contentResult.Content = await new StreamReader(stream).ReadToEndAsync();

                return contentResult;
            }

            log.LogError($"Model not founded: {modelId}");

            return new NotFoundResult();
        }
    }
}

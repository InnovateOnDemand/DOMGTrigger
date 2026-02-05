using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;
using System.Text;

namespace Trigger
{
    public static class FBAudienceExtract
    {
        [FunctionName("ExtractBigQueryDataQueue")]
        public static async Task RunQueue(
            [QueueTrigger("extract-queue", Connection = "AzureWebJobsStorage")]
            string message,
            ILogger log)
        {
            log.LogInformation("===== ExtractBigQueryDataFunction START =====");
            
            // 1. Decode message from Base64 if needed
            string jsonMessage = message;
            try
            {
                byte[] data = Convert.FromBase64String(message);
                jsonMessage = Encoding.UTF8.GetString(data);
                log.LogInformation("Message decoded from Base64");
            }
            catch
            {
                log.LogInformation("Message is not Base64 encoded, using as-is");
            }
            
            // 2. Deserializing the message
            var payload = JsonConvert.DeserializeObject<ExtractAudiencePayload>(jsonMessage);
            
            // Validate payload
            if (payload == null || string.IsNullOrEmpty(payload.AudienceId))
            {
                log.LogError("Invalid or null payload received. Cannot process.");
                throw new ArgumentException("Payload is null or missing AudienceId");
            }
            
            log.LogInformation($"Processing audience: {payload.AudienceId} - {payload.AudienceName}");
            try
            {
                // 2. Extracting data from BigQuery
                var customerData = GetCustomerDataFromBigQuery(payload.Sql, log);
                if (customerData.Count == 0)
                {
                    log.LogInformation("No data found from BigQuery. Exiting function...");
                    return;
                }
                // 3. Saving data in Blob
                string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                if (string.IsNullOrEmpty(storageConnectionString))
                {
                    log.LogError("ERROR: AzureWebJobsStorage environment variable is not set");
                    throw new InvalidOperationException("Missing AzureWebJobsStorage configuration");
                }
                
                string containerName = string.IsNullOrEmpty(payload.ContainerName)
                                        ? "fb-audiences-data"
                                        : payload.ContainerName;
                log.LogInformation($"Uploading data to Blob container: {containerName}");
                
                var blobPaths = await SaveCustomerDataToBlobAsync(
                    payload.AudienceId,
                    customerData,
                    containerName,
                    storageConnectionString);
                log.LogInformation($"Total records: {customerData.Count}, Blobs created: {blobPaths.Count}");
                if (blobPaths.Count == 0)
                {
                    log.LogInformation("No valid blobs created. Possibly no data. Exiting function...");
                    return;
                }
                // 4. Enqueue message for the Function "Populate" or "Replace", according to IsReplace
                string nextQueueName = payload.IsReplace ? "replace-queue" : "populate-queue";
                log.LogInformation($"Enqueueing message to: {nextQueueName}");
                
                QueueClient queueClient = new QueueClient(storageConnectionString, nextQueueName);
                await queueClient.CreateIfNotExistsAsync();
                var nextPayload = new PopulateQueuePayload
                {
                    AudienceId = payload.AudienceId,
                    AudienceName = payload.AudienceName,
                    FacebookAccessToken = payload.FacebookAccessToken,
                    ContainerName = containerName,
                    BlobPaths = blobPaths,
                    UserEmail = payload.UserEmail
                };
                string nextJson = JsonConvert.SerializeObject(nextPayload);
                string nextJsonBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(nextJson));
                await queueClient.SendMessageAsync(nextJsonBase64);
                log.LogInformation($"Message enqueued to {nextQueueName}. Extraction completed successfully.");
            }
            catch (Exception ex)
            {
                log.LogError($"ERROR in ExtractBigQueryDataFunction: {ex.Message}");
                log.LogError($"StackTrace: {ex.StackTrace}");
                
                // Notify the error by email
                string userEmail = payload?.UserEmail ?? "fernando.pavia@innovateod.com";
                string audienceInfo = payload != null 
                    ? $"{payload.AudienceId} - {payload.AudienceName}" 
                    : "Unknown (failed to deserialize payload)";
                
                await helper.SendMail(userEmail,
                    "Error extracting Customer data for Facebook Audience",
                    $"An error happened while extracting the data for the FB Audience: {audienceInfo}" +
                    $"\nError message: {ex.Message}\nStackTrace:\n{ex.StackTrace}");
            }
        }

        private static List<Dictionary<string, object>> GetCustomerDataFromBigQuery(string sql, ILogger log)
        {
            log.LogInformation("Extracting data from BigQuery...");
            
            // Validate environment variables
            string BQprojectName = Environment.GetEnvironmentVariable("BigQueryProjectName");
            string BQdatasetName = Environment.GetEnvironmentVariable("BigQueryDatasetName");
            string jsonCreds = Environment.GetEnvironmentVariable("GOOGLE_CREDENTIALS_JSON");
            
            if (string.IsNullOrEmpty(BQprojectName))
            {
                log.LogError("ERROR: BigQueryProjectName environment variable is not set");
                throw new InvalidOperationException("Missing BigQueryProjectName configuration");
            }
            if (string.IsNullOrEmpty(BQdatasetName))
            {
                log.LogError("ERROR: BigQueryDatasetName environment variable is not set");
                throw new InvalidOperationException("Missing BigQueryDatasetName configuration");
            }
            if (string.IsNullOrEmpty(jsonCreds))
            {
                log.LogError("ERROR: GOOGLE_CREDENTIALS_JSON environment variable is not set");
                throw new InvalidOperationException("Missing Google credentials configuration");
            }
            
            log.LogInformation($"Using BigQuery project: {BQprojectName}");

            try
            {
                // 1. Credentials            
                var credential = GoogleCredential.FromJson(jsonCreds);

                // 2. Creating cliente
                var client = BigQueryClient.Create(BQprojectName, credential);

                // 3. Running query
                log.LogInformation("Executing BigQuery...");
                BigQueryResults results = client.ExecuteQuery(sql, parameters: null);

                // 4. Mapping to List<Dictionary<string, object>>
                var customerData = new List<Dictionary<string, object>>();
                int rowCount = 0;
                foreach (var row in results)
                {
                    var dict = new Dictionary<string, object>
                    {
                        { "email1", row["email1"]?.ToString() },
                        { "email2", row["email2"]?.ToString() },
                        { "email3", row["email3"]?.ToString() },
                        { "phone1", row["phone1"]?.ToString() },
                        { "phone2", row["phone2"]?.ToString() },
                        { "phone3", row["phone3"]?.ToString() },
                        { "fn", row["fn"]?.ToString() },
                        { "ln", row["ln"]?.ToString() },
                        { "zip", row["zip"]?.ToString() },
                        { "ct", row["ct"]?.ToString() },
                        { "st", row["st"]?.ToString() },
                        { "country", row["country"]?.ToString() },
                        { "doby", row["doby"]?.ToString() },
                        { "gen", row["gen"]?.ToString() },
                        { "age", row["age"]?.ToString() }
                    };
                    customerData.Add(dict);
                    rowCount++;
                }
                log.LogInformation($"Successfully extracted {rowCount} rows from BigQuery");
                return customerData;
            }
            catch (Exception ex)
            {
                log.LogError($"ERROR executing BigQuery: {ex.Message}");
                log.LogError($"StackTrace: {ex.StackTrace}");
                throw;
            }
        }        

        private static async Task<List<string>> SaveCustomerDataToBlobAsync(
            string audienceId,
            List<Dictionary<string, object>> customerData,
            string containerName,
            string storageConnectionString)
        {
            var blobPaths = new List<string>();
            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(containerName);
            await containerClient.CreateIfNotExistsAsync(PublicAccessType.None);
            int maxPerFile = 50000;
            if (customerData.Count <= maxPerFile)
            {
                string blobName = $"{audienceId}/{audienceId}.json";
                var blobClient = containerClient.GetBlobClient(blobName);
                string json = JsonConvert.SerializeObject(customerData);
                using (var ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)))
                {
                    await blobClient.UploadAsync(ms, overwrite: true);
                }
                blobPaths.Add(blobName);
            }
            else
            {
                int chunkSize = maxPerFile;
                int total = customerData.Count;
                int index = 0;
                int fileNum = 1;
                while (index < total)
                {
                    var chunk = customerData.Skip(index).Take(chunkSize).ToList();
                    string blobName = $"{audienceId}/{audienceId}_chunk{fileNum}.json";
                    var blobClient = containerClient.GetBlobClient(blobName);
                    string json = JsonConvert.SerializeObject(chunk);
                    using (var ms = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)))
                    {
                        await blobClient.UploadAsync(ms, overwrite: true);
                    }
                    blobPaths.Add(blobName);
                    index += chunkSize;
                    fileNum++;
                }
            }
            return blobPaths;
        }
    }

    // El payload que recibimos en la cola "extract-queue"
    public class ExtractAudiencePayload
    {
        public string AudienceId { get; set; }
        public string AudienceName { get; set; }
        public string Sql { get; set; }
        public string FacebookAccessToken { get; set; }
        public bool IsReplace { get; set; }
        public string ContainerName { get; set; }
        public string UserEmail { get; set; }
    }
    // El payload que enviamos a la cola de "populate" o "replace"
    public class PopulateQueuePayload
    {
        public string AudienceId { get; set; }
        public string AudienceName { get; set; }
        public string FacebookAccessToken { get; set; }
        public string ContainerName { get; set; }
        public List<string> BlobPaths { get; set; }
        public string UserEmail { get; set; }
    }
}
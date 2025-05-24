using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

using Azure.Storage.Blobs;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Azure.Storage.Queues;
using System.Text;

namespace Trigger
{
    public static class FBAudienceReplace
    {
        [FunctionName("ReplaceFacebookAudienceQueue")]
        public static async Task RunQueue(
            [QueueTrigger("replace-queue", Connection = "AzureWebJobsStorage")]
            string message,
            ILogger log)
        {
            log.LogInformation("===== ReplaceFacebookAudienceFunction START =====");

            var payload = JsonConvert.DeserializeObject<PopulateAudiencePayload>(message);

            try
            {
                // 1. Connecting to Blob
                string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);
                BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(payload.ContainerName);

                // 2. Combining all the data in memory
                var allCustomers = new List<Dictionary<string, object>>();
                foreach (var blobPath in payload.BlobPaths)
                {
                    var blobClient = containerClient.GetBlobClient(blobPath);
                    if (!await blobClient.ExistsAsync())
                    {
                        log.LogWarning($"Blob {blobPath} not found. Skipping.");
                        continue;
                    }
                    var downloadResult = await blobClient.DownloadContentAsync();
                    var jsonContent = downloadResult.Value.Content.ToString();
                    var chunk = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jsonContent);
                    if (chunk?.Count > 0) allCustomers.AddRange(chunk);
                }

                if (allCustomers.Count == 0)
                {
                    log.LogWarning("No data found in all blobs. Exiting replace function.");
                    // Delete blobs
                    foreach (var bp in payload.BlobPaths)
                    {
                        await containerClient.GetBlobClient(bp)
                            .DeleteIfExistsAsync(Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots);
                    }
                    return;
                }

                // 3. Make the /usersreplace with sub-batches
                string replaceApiUrl = $"https://graph.facebook.com/v22.0/{payload.AudienceId}/usersreplace?access_token={payload.FacebookAccessToken}";

                var audienceUpdates = new Dictionary<string, object>
                {
                    { "session_id", "" },
                    { "num_received", 0 },
                    { "num_invalid_entries", 0 },
                    { "invalid_entry_samples", new JArray() }
                };

                long sessionId = DateTime.UtcNow.Ticks;
                int chunkSize = 5000;
                int batchSeq = 1;

                using (var httpClient = new HttpClient())
                {
                    for (int i = 0; i < allCustomers.Count; i += chunkSize)
                    {
                        var subChunk = allCustomers.Skip(i).Take(chunkSize).ToList();

                        var dataForFacebook = helper.PrepareDataForFacebook(subChunk);
                        var payloadData = new
                        {
                            schema = new[] {
                            "EMAIL", "EMAIL", "EMAIL",
                            "PHONE", "PHONE", "PHONE",
                            "FN", "LN", "ZIP",
                            "CT", "ST", "COUNTRY",
                            "DOBY", "GEN"
                        },
                            data = dataForFacebook
                        };

                        bool isLastBatch = (i + chunkSize >= allCustomers.Count);

                        var sessionObj = new
                        {
                            session_id = sessionId,
                            batch_seq = batchSeq,
                            last_batch_flag = isLastBatch,
                            estimated_num_total = allCustomers.Count
                        };

                        using (var content = new MultipartFormDataContent())
                        {
                            content.Add(new StringContent(JsonConvert.SerializeObject(payloadData)), "payload");
                            content.Add(new StringContent(JsonConvert.SerializeObject(sessionObj)), "session");

                            var response = await httpClient.PostAsync(replaceApiUrl, content);
                            if (!response.IsSuccessStatusCode)
                            {
                                var errorContent = await response.Content.ReadAsStringAsync();
                                throw new HttpRequestException($"Facebook API Error: {errorContent}");
                            }

                            var responseContent = await response.Content.ReadAsStringAsync();
                            var result = JsonConvert.DeserializeObject<JObject>(responseContent);

                            audienceUpdates["session_id"] = result["session_id"]?.ToString();
                            audienceUpdates["num_received"] = (int)audienceUpdates["num_received"] + (int)result["num_received"];
                            audienceUpdates["num_invalid_entries"] = (int)audienceUpdates["num_invalid_entries"] + (int)result["num_invalid_entries"];
                            ((JArray)audienceUpdates["invalid_entry_samples"]).Merge(result["invalid_entry_samples"]);
                        }
                        batchSeq++;
                    }
                }
                // 4. Deleting blobs
                log.LogInformation("Deleting blobs after replace...");
                foreach (var bp in payload.BlobPaths)
                {
                    await containerClient.GetBlobClient(bp)
                        .DeleteIfExistsAsync(Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots);
                }

                // 5. Notify by Email
                var summary = $"num_received: {audienceUpdates["num_received"]}, " +
                              $"num_invalid_entries: {audienceUpdates["num_invalid_entries"]}";
                await helper.SendMail(payload.UserEmail,
                    "Audience Update Completed",
                    $"The update process for the FB Audience: {payload.AudienceId} - {payload.AudienceName}, was completed succesfully." +
                    $"\nSummary: {summary}");

                
                // 6. Log
                log.LogInformation($"Replace done for {payload.AudienceId}. Received={audienceUpdates["num_received"]}, invalid={audienceUpdates["num_invalid_entries"]}");

                // +++ START: Enqueue message for status check +++
                try
                {
                    string statusQueueName = "status-check-queue";
                    QueueClient statusQueueClient = new QueueClient(storageConnectionString, statusQueueName);
                    await statusQueueClient.CreateIfNotExistsAsync();

                    int expectedSize = (int)audienceUpdates["num_received"] - (int)audienceUpdates["num_invalid_entries"];
                    // Ensure expectedSize is not negative
                    expectedSize = Math.Max(0, expectedSize);

                    var statusPayload = new StatusCheckPayload
                    {
                        AudienceId = payload.AudienceId,
                        AudienceName = payload.AudienceName,
                        UserEmail = payload.UserEmail,
                        ExpectedSize = expectedSize
                    };

                    string statusJson = JsonConvert.SerializeObject(statusPayload);
                    // Use SendMessageAsync overload with visibilityTimeout
                    TimeSpan delay = TimeSpan.FromMinutes(150); // 150 minutes delay
                    await statusQueueClient.SendMessageAsync(Convert.ToBase64String(Encoding.UTF8.GetBytes(statusJson)), visibilityTimeout: delay);

                    log.LogInformation($"Enqueued status check message for Audience ID {payload.AudienceId} with a {delay.TotalMinutes}-minute delay.");
                }
                catch (Exception queueEx)
                {
                    log.LogError(queueEx, $"Failed to enqueue status check message for Audience ID {payload.AudienceId}.");
                    // Decide if this error should be critical or just logged.
                }
                // +++ END: Enqueue message for status check +++

                log.LogInformation("===== ReplaceFacebookAudienceFunction END =====");
            } catch(Exception ex)
            {
                // Notify the error by email
                await helper.SendMail(payload.UserEmail,
                    "Error replacing Facebook Audience",
                    $"An error happened while populating the FB Audience: {payload.AudienceId} - {payload.AudienceName}" +
                    $"\nError message: {ex.Message}\nStackTrace:\n{ex.StackTrace}");

                // Error handling: try to clean blobs
                await helper.HandleErrorAndCleanUpBlobs(ex, message);

                log.LogError(ex, "Error in ReplaceFacebookAudienceQueue");
            }
            
            log.LogInformation("===== ReplaceFacebookAudienceFunction END =====");
        }
    }

    /// <summary>
    /// Model to receive parameters in ReplaceFacebookAudienceFunction
    /// </summary>
    public class ReplaceAudiencePayload
    {
        public string AudienceId { get; set; }
        public string AudienceName { get; set; }
        public string FacebookAccessToken { get; set; }

        // Blob container and paths where data was uploaded
        public string ContainerName { get; set; }
        public List<string> BlobPaths { get; set; }
        public string UserEmail { get; set; }
    }
}

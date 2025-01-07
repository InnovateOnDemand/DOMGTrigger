using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Azure.Storage.Blobs;

namespace Trigger
{
    public static class FBAudienceReplace
    {
        [FunctionName("ReplaceFacebookAudienceFunction")]
        public static async Task Run(
            [QueueTrigger("replace-queue", Connection = "AzureWebJobsStorage")]
            string message,
            ILogger log)
        {
            log.LogInformation("===== ReplaceFacebookAudienceFunction START =====");

            var payload = JsonConvert.DeserializeObject<PopulateQueuePayload>(message);

            // 1. Conectarse a Blob
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(payload.ContainerName);

            // 2. Combinar toda la data en memoria (segun tu “replace” approach)
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
                // Borramos blobs
                foreach (var bp in payload.BlobPaths)
                {
                    await containerClient.GetBlobClient(bp)
                        .DeleteIfExistsAsync(Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots);
                }
                return;
            }

            // 3. Realizar el /usersreplace con sub-lotes
            string replaceApiUrl = $"https://graph.facebook.com/v20.0/{payload.AudienceId}/usersreplace?access_token={payload.FacebookAccessToken}";

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

                    var dataForFacebook = PrepareDataForFacebook(subChunk);
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

            // 4. Borrar blobs
            log.LogInformation("Deleting blobs after replace...");
            foreach (var bp in payload.BlobPaths)
            {
                await containerClient.GetBlobClient(bp)
                    .DeleteIfExistsAsync(Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots);
            }

            // 5. Log/Notificar
            log.LogInformation($"Replace done for {payload.AudienceId}. Received={audienceUpdates["num_received"]}, invalid={audienceUpdates["num_invalid_entries"]}");
            log.LogInformation("===== ReplaceFacebookAudienceFunction END =====");
        }

        private static List<List<string>> PrepareDataForFacebook(List<Dictionary<string, object>> subChunk)
        {
            var finalList = new List<List<string>>();
            foreach (var customer in subChunk)
            {
                var row = new List<string>
                {
                    customer.ContainsKey("email1") ? customer["email1"]?.ToString() : "",
                    customer.ContainsKey("email2") ? customer["email2"]?.ToString() : "",
                    customer.ContainsKey("email3") ? customer["email3"]?.ToString() : "",
                    customer.ContainsKey("phone1") ? customer["phone1"]?.ToString() : "",
                    customer.ContainsKey("phone2") ? customer["phone2"]?.ToString() : "",
                    customer.ContainsKey("phone3") ? customer["phone3"]?.ToString() : "",
                    customer.ContainsKey("fn") ? customer["fn"]?.ToString() : "",
                    customer.ContainsKey("ln") ? customer["ln"]?.ToString() : "",
                    customer.ContainsKey("zip") ? customer["zip"]?.ToString() : "",
                    customer.ContainsKey("ct") ? customer["ct"]?.ToString() : "",
                    customer.ContainsKey("st") ? customer["st"]?.ToString() : "",
                    customer.ContainsKey("country") ? customer["country"]?.ToString() : "",
                    customer.ContainsKey("doby") ? customer["doby"]?.ToString() : "",
                    customer.ContainsKey("gen") ? customer["gen"]?.ToString() : "",
                };
                finalList.Add(row);
            }
            return finalList;
        }
    }
}

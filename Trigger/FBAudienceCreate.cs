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
    public static class FBAudienceCreate
    {
        [FunctionName("PopulateFacebookAudienceFunction")]
        public static async Task Run(
            [QueueTrigger("populate-queue", Connection = "AzureWebJobsStorage")]
            string message,
            ILogger log)
        {
            log.LogInformation("===== PopulateFacebookAudienceFunction START =====");

            var payload = JsonConvert.DeserializeObject<PopulateQueuePayload>(message);

            // 1. Conectarse al Storage
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(payload.ContainerName);

            var audienceUpdates = new Dictionary<string, object>
            {
                { "audience_id", payload.AudienceId },
                { "session_id", "" },
                { "num_received", 0 },
                { "num_invalid_entries", 0 },
                { "invalid_entry_samples", new JArray() }
            };

            using (var httpClient = new HttpClient())
            {
                // 2. Por cada blob en la lista, descargar, deserializar, subir a FB
                foreach (var blobPath in payload.BlobPaths)
                {
                    log.LogInformation($"Downloading blob: {blobPath}");
                    var blobClient = containerClient.GetBlobClient(blobPath);
                    if (!await blobClient.ExistsAsync())
                    {
                        log.LogWarning($"Blob {blobPath} does not exist. Skipping.");
                        continue;
                    }

                    var downloadResult = await blobClient.DownloadContentAsync();
                    var jsonContent = downloadResult.Value.Content.ToString();

                    var customersChunk = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jsonContent);
                    if (customersChunk == null || customersChunk.Count == 0)
                    {
                        log.LogWarning($"Blob {blobPath} is empty or invalid.");
                        continue;
                    }

                    // Subdividir en 9999
                    int chunkSize = 9999;
                    var subChunks = customersChunk
                        .Select((c, idx) => new { c, idx })
                        .GroupBy(x => x.idx / chunkSize)
                        .Select(g => g.Select(x => x.c).ToList())
                        .ToList();

                    // Llamar a FB
                    string addUsersApiUrl = $"https://graph.facebook.com/v20.0/{payload.AudienceId}/users";

                    foreach (var subChunk in subChunks)
                    {
                        var dataForFacebook = PrepareDataForFacebook(subChunk);
                        var schema = new List<string> {
                            "EMAIL","EMAIL","EMAIL",
                            "PHONE","PHONE","PHONE",
                            "FN","LN","ZIP",
                            "CT","ST","COUNTRY",
                            "DOBY","GEN"
                        };

                        var addUsersPayload = new
                        {
                            schema = schema,
                            data = dataForFacebook
                        };

                        using (var addUsersContent = new MultipartFormDataContent())
                        {
                            addUsersContent.Add(new StringContent(JsonConvert.SerializeObject(addUsersPayload)), "payload");
                            addUsersContent.Add(new StringContent(payload.FacebookAccessToken), "access_token");

                            var response = await httpClient.PostAsync(addUsersApiUrl, addUsersContent);
                            if (!response.IsSuccessStatusCode)
                            {
                                var errorContent = await response.Content.ReadAsStringAsync();
                                throw new HttpRequestException($"Facebook API Error: {errorContent}");
                            }

                            var addUsersResponseContent = await response.Content.ReadAsStringAsync();
                            var addUsersResult = JsonConvert.DeserializeObject<JObject>(addUsersResponseContent);

                            audienceUpdates["session_id"] = addUsersResult["session_id"]?.ToString();
                            audienceUpdates["num_received"] = (int)audienceUpdates["num_received"] + (int)addUsersResult["num_received"];
                            audienceUpdates["num_invalid_entries"] = (int)audienceUpdates["num_invalid_entries"] + (int)addUsersResult["num_invalid_entries"];
                            ((JArray)audienceUpdates["invalid_entry_samples"]).Merge(addUsersResult["invalid_entry_samples"]);
                        }
                    }
                }
            }

            // 3. Borrar blobs
            log.LogInformation("Deleting associated blobs...");
            foreach (var blobPath in payload.BlobPaths)
            {
                var blobClient = containerClient.GetBlobClient(blobPath);
                await blobClient.DeleteIfExistsAsync(Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots);
            }

            // 4. Notificar por correo, logs, etc.
            // e.g. log info or call an email service
            log.LogInformation($"Populate completed for audience {payload.AudienceId}. num_received={audienceUpdates["num_received"]}, invalid={audienceUpdates["num_invalid_entries"]}");

            log.LogInformation("===== PopulateFacebookAudienceFunction END =====");
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
                    customer.ContainsKey("gen") ? customer["gen"]?.ToString() : ""
                };
                finalList.Add(row);
            }
            return finalList;
        }
    }
}

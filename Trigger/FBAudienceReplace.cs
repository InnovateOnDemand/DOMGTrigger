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

        //[FunctionName("ReplaceFacebookAudienceFunction")]
        //public static async Task<IActionResult> Run(
        //    [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)]
        //    HttpRequest req,
        //    ILogger log)
        //{
        //    // Email address for notifications.
        //    const string notificationEmail = "lenin.carrasco@innovateod.com";

        //    // Tomamos el connection string de Storage desde variables de entorno
        //    string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

        //    // 1. Leer payload del body
        //    string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
        //    var payload = JsonConvert.DeserializeObject<ReplaceAudiencePayload>(requestBody);

        //    _ = Task.Run(async () =>
        //    {
        //        try
        //        {
        //            log.LogInformation("===== ReplaceFacebookAudienceFunction START =====");

        //            // Validaciones mínimas
        //            if (string.IsNullOrEmpty(payload.AudienceId) ||
        //                string.IsNullOrEmpty(payload.FacebookAccessToken) ||
        //                string.IsNullOrEmpty(payload.ContainerName) ||
        //                payload.BlobPaths == null || payload.BlobPaths.Count == 0)
        //            {
        //                return new BadRequestObjectResult("Faltan datos en el payload (AudienceId, FacebookAccessToken, ContainerName, BlobPaths).");
        //            }

        //            string audienceId = payload.AudienceId;
        //            string fbAccessToken = payload.FacebookAccessToken;
        //            string containerName = payload.ContainerName;
        //            var blobPaths = payload.BlobPaths;

        //            log.LogInformation($"Replace audienceId: {audienceId}, containerName: {containerName}, totalBlobs: {blobPaths.Count}");

        //            // 2. Descargamos y combinamos TODOS los datos en una sola lista
        //            var allCustomers = new List<Dictionary<string, object>>();

        //            BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);
        //            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

        //            foreach (var blobPath in blobPaths)
        //            {
        //                var blobClient = containerClient.GetBlobClient(blobPath);

        //                if (!await blobClient.ExistsAsync())
        //                {
        //                    log.LogWarning($"El blob {blobPath} no existe. Se omite.");
        //                    continue;
        //                }

        //                // Descargar contenido
        //                var downloadResult = await blobClient.DownloadContentAsync();
        //                var jsonContent = downloadResult.Value.Content.ToString();

        //                // Deserializa
        //                var customersChunk = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jsonContent);
        //                if (customersChunk != null && customersChunk.Count > 0)
        //                {
        //                    allCustomers.AddRange(customersChunk);
        //                }
        //            }

        //            if (allCustomers.Count == 0)
        //            {
        //                log.LogWarning("No se encontró data de clientes en los blobs (o estaban vacíos).");
        //                // Eliminamos blobs y salimos
        //                foreach (var blobPath in blobPaths)
        //                {
        //                    await containerClient.GetBlobClient(blobPath)
        //                        .DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
        //                }
        //                return new OkObjectResult("No customer data found to replace.");
        //            }

        //            // 3. Preparamos la llamada a /usersreplace? en UNA sola sesión
        //            string replaceUsersApiUrl = $"https://graph.facebook.com/v20.0/{audienceId}/usersreplace?access_token={fbAccessToken}";

        //            // Diccionario para acumular resultados
        //            var audienceUpdates = new Dictionary<string, object>
        //            {
        //                { "session_id", "" },
        //                { "num_received", 0 },
        //                { "num_invalid_entries", 0 },
        //                { "invalid_entry_samples", new JArray() }
        //            };

        //            // Mismo sessionId para TODOS los sub-lotes
        //            long sessionId = DateTime.UtcNow.Ticks;

        //            // 4. Llamadas en sub-lotes (batch_seq incrementa)
        //            int chunkSize = 5000;
        //            int batchSeq = 1;

        //            using (var httpClient = new HttpClient())
        //            {
        //                for (int i = 0; i < allCustomers.Count; i += chunkSize)
        //                {
        //                    // Sub-lote
        //                    var subChunk = allCustomers.Skip(i).Take(chunkSize).ToList();

        //                    // Preparar data
        //                    var dataForFacebook = helper.PrepareDataForFacebook(subChunk);

        //                    var payloadData = new
        //                    {
        //                        schema = new[]
        //                        {
        //                        "EMAIL", "EMAIL", "EMAIL",
        //                        "PHONE", "PHONE", "PHONE",
        //                        "FN", "LN", "ZIP",
        //                        "CT", "ST", "COUNTRY",
        //                        "DOBY", "GEN"
        //                    },
        //                        data = dataForFacebook
        //                    };

        //                    bool isLastBatch = (i + chunkSize >= allCustomers.Count);

        //                    var sessionObj = new
        //                    {
        //                        session_id = sessionId,
        //                        batch_seq = batchSeq,
        //                        last_batch_flag = isLastBatch,
        //                        estimated_num_total = allCustomers.Count
        //                    };

        //                    using (var content = new MultipartFormDataContent())
        //                    {
        //                        content.Add(new StringContent(JsonConvert.SerializeObject(payloadData)), "payload");
        //                        content.Add(new StringContent(JsonConvert.SerializeObject(sessionObj)), "session");

        //                        // Invocar Facebook
        //                        var response = await httpClient.PostAsync(replaceUsersApiUrl, content);
        //                        if (!response.IsSuccessStatusCode)
        //                        {
        //                            var errorContent = await response.Content.ReadAsStringAsync();
        //                            throw new HttpRequestException($"Facebook API Error: {errorContent}");
        //                        }

        //                        var responseContent = await response.Content.ReadAsStringAsync();
        //                        var result = JsonConvert.DeserializeObject<JObject>(responseContent);

        //                        // Actualizar contadores
        //                        audienceUpdates["session_id"] = result["session_id"]?.ToString();
        //                        audienceUpdates["num_received"] = (int)audienceUpdates["num_received"] + (int)result["num_received"];
        //                        audienceUpdates["num_invalid_entries"] = (int)audienceUpdates["num_invalid_entries"] + (int)result["num_invalid_entries"];
        //                        ((JArray)audienceUpdates["invalid_entry_samples"]).Merge(result["invalid_entry_samples"]);
        //                    }

        //                    batchSeq++;
        //                }
        //            }

        //            // 5. Borrar blobs asociados
        //            log.LogInformation("Eliminando blobs asociados a la audiencia...");
        //            foreach (var blobPath in blobPaths)
        //            {
        //                var blobClient = containerClient.GetBlobClient(blobPath);
        //                await blobClient.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
        //            }

        //            // 6. Notify by Email
        //            var summary = $"num_received: {audienceUpdates["num_received"]}, " +
        //                          $"num_invalid_entries: {audienceUpdates["num_invalid_entries"]}";
        //            await helper.SendMail(notificationEmail,
        //                "Audience Update Completed",
        //                $"The update process for the FB Audience: {payload.AudienceId} - {payload.AudienceName}, was completed succesfully." +
        //                $"\nSummary: {summary}");

        //            log.LogInformation("===== ReplaceFacebookAudienceFunction END =====");

        //            return new OkObjectResult(audienceUpdates);
        //        }
        //        catch (Exception ex)
        //        {
        //            // Error handling: try to clean blobs
        //            // await HandleErrorAndCleanUpBlobs(ex, req);

        //            // Notify the error by email
        //            await helper.SendMail(payload.UserEmail,
        //                "Error replacing Facebook Audience",
        //                $"An error happened while populating the FB Audience: {payload.AudienceId} - {payload.AudienceName}" +
        //                $"\nError message: {ex.Message}\nStackTrace:\n{ex.StackTrace}");

        //            return new ObjectResult($"Error en ReplaceFacebookAudienceFunction: {ex.Message}")
        //            {
        //                StatusCode = StatusCodes.Status500InternalServerError
        //            };
        //        }
        //    });

        //    return new AcceptedResult();
        //}
    }

    /// <summary>
    /// Modelo para recibir parámetros en ReplaceFacebookAudienceFunction
    /// </summary>
    public class ReplaceAudiencePayload
    {
        public string AudienceId { get; set; }
        public string AudienceName { get; set; }
        public string FacebookAccessToken { get; set; }

        // Blob container y paths donde se guardaron los datos
        public string ContainerName { get; set; }
        public List<string> BlobPaths { get; set; }
        public string UserEmail { get; set; }
    }
}

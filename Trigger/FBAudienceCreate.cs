﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;

using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Trigger
{
    public static class FBAudienceCreate
    {
        [FunctionName("PopulateFacebookAudienceFunction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)]
            HttpRequest req,
            ILogger log)
        {
            // Email address for notifications.
            const string notificationEmail = "lenin.carrasco@innovateod.com";

            // Take the connection string of Storage from settings:
            // (Normally it is defined in "AzureWebJobsStorage" or in another environment variable.)
            string storageConnectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");

            // 1. Read and deserialize the request payload
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var payload = JsonConvert.DeserializeObject<PopulateAudiencePayload>(requestBody);

            _ = Task.Run(async () =>
            {
                try
                {
                    log.LogInformation("===== PopulateFacebookAudienceFunction START =====");

                    // 2. Extract the data from the payload
                    string audienceId = payload.AudienceId;
                    string fbAccessToken = payload.FacebookAccessToken;
                    string containerName = payload.ContainerName;
                    List<string> blobPaths = payload.BlobPaths;
                    // blobPaths will contain one or several "files" (e.g., "audience_123/audience_123_chunk_1.json", etc.)

                    if (string.IsNullOrEmpty(audienceId) ||
                        string.IsNullOrEmpty(fbAccessToken) ||
                        string.IsNullOrEmpty(containerName) ||
                        blobPaths == null || blobPaths.Count == 0)
                    {
                        return new BadRequestObjectResult("Missing data in the payload (AudienceId, FB token, containerName, or blobPaths).");
                    }

                    log.LogInformation($"audienceId: {audienceId}, containerName: {containerName}, totalBlobs: {blobPaths.Count}");

                    // 3. Prepare an object to gather global results
                    var audienceUpdates = new Dictionary<string, object>
                {
                    { "audience_id", audienceId },
                    { "session_id", "" },
                    { "num_received", 0 },
                    { "num_invalid_entries", 0 },
                    { "invalid_entry_samples", new JArray() }
                };

                    // 4. Connect to Blob Storage
                    BlobServiceClient blobServiceClient = new BlobServiceClient(storageConnectionString);
                    BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);

                    // 5. Process each blob that contains part of the customer data
                    using (var httpClient = new HttpClient())
                    {
                        foreach (var blobPath in blobPaths)
                        {
                            log.LogInformation($"Descargando blob: {blobPath}");
                            var blobClient = containerClient.GetBlobClient(blobPath);

                            if (!await blobClient.ExistsAsync())
                            {
                                log.LogWarning($"El blob {blobPath} no existe. Se omite.");
                                continue;
                            }

                            // Download content
                            var downloadResult = await blobClient.DownloadContentAsync();
                            var jsonContent = downloadResult.Value.Content.ToString();

                            // Deserialize the customer list
                            var customersChunk = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(jsonContent);
                            if (customersChunk == null || customersChunk.Count == 0)
                            {
                                log.LogWarning($"El blob {blobPath} no contenía datos de clientes.");
                                continue;
                            }

                            // Subdivide into batches of 9999 for Facebook
                            int chunkSize = 9999;
                            var subChunks = customersChunk
                                .Select((c, idx) => new { c, idx })
                                .GroupBy(x => x.idx / chunkSize)
                                .Select(g => g.Select(x => x.c).ToList())
                                .ToList();

                            // Upload each sub-batch to Facebook
                            string addUsersApiUrl = $"https://graph.facebook.com/v20.0/{audienceId}/users";

                            foreach (var subChunk in subChunks)
                            {
                                // Prepare data for Facebook (adjust as needed)
                                var dataForFacebook = PrepareDataForFacebook(subChunk);

                                var schema = new List<string>
                                {
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
                                    addUsersContent.Add(new StringContent(fbAccessToken), "access_token");

                                    var addUsersResponse = await httpClient.PostAsync(addUsersApiUrl, addUsersContent);
                                    if (addUsersResponse.IsSuccessStatusCode)
                                    {
                                        var addUsersResponseContent = await addUsersResponse.Content.ReadAsStringAsync();
                                        var addUsersResult = JsonConvert.DeserializeObject<JObject>(addUsersResponseContent);

                                        audienceUpdates["session_id"] = addUsersResult["session_id"]?.ToString();
                                        audienceUpdates["num_received"] = (int)audienceUpdates["num_received"] + (int)addUsersResult["num_received"];
                                        audienceUpdates["num_invalid_entries"] = (int)audienceUpdates["num_invalid_entries"] + (int)addUsersResult["num_invalid_entries"];
                                        ((JArray)audienceUpdates["invalid_entry_samples"]).Merge(addUsersResult["invalid_entry_samples"]);
                                    }
                                    else
                                    {
                                        var errorContent = await addUsersResponse.Content.ReadAsStringAsync();
                                        throw new HttpRequestException($"Error en Facebook API: {errorContent}");
                                    }
                                }
                            }
                        }
                    }

                    // 6. Delete the blobs corresponding to this audience
                    //    (the ones provided in `blobPaths`)
                    log.LogInformation("Eliminando blobs asociados a la audiencia...");
                    foreach (var blobPath in blobPaths)
                    {
                        var blobClient = containerClient.GetBlobClient(blobPath);
                        await blobClient.DeleteIfExistsAsync(Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots);
                    }
                    log.LogInformation("Blobs eliminados correctamente.");

                    // 7. Send notification email (success)
                    //    You could include data such as the total processed, etc.
                    var summary = $"Audiencia: {audienceId}\n" +
                                  $"num_received: {audienceUpdates["num_received"]}\n" +
                                  $"num_invalid_entries: {audienceUpdates["num_invalid_entries"]}\n";

                    await SendMail(notificationEmail,
                        "Carga de audiencia finalizada con éxito",
                        $"Se ha completado el proceso de poblado para la audiencia {audienceId}.\n{summary}");

                    // Final response OK
                    log.LogInformation("===== PopulateFacebookAudienceFunction FIN =====");
                    return new OkObjectResult(audienceUpdates);
                }
                catch (Exception ex)
                {
                    // In case of an error, you could also delete the blobs or leave them for debugging.
                    // You could also notify the error by email.
                    // Here, we'll do a simple deletion and notification.

                    try
                    {
                        // Try to extract the payload to identify the blobs
                        req.Body.Position = 0; // reset stream 
                        string requestBody2 = await new StreamReader(req.Body).ReadToEndAsync();
                        var payload2 = JsonConvert.DeserializeObject<PopulateAudiencePayload>(requestBody2);

                        if (payload2 != null &&
                            !string.IsNullOrEmpty(payload2.ContainerName) &&
                            payload2.BlobPaths?.Count > 0)
                        {
                            // Delete the blobs
                            var blobServiceClient = new BlobServiceClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"));
                            var containerClient = blobServiceClient.GetBlobContainerClient(payload2.ContainerName);

                            foreach (var blobPath in payload2.BlobPaths)
                            {
                                var blobClient = containerClient.GetBlobClient(blobPath);
                                await blobClient.DeleteIfExistsAsync(Azure.Storage.Blobs.Models.DeleteSnapshotsOption.IncludeSnapshots);
                            }
                        }
                    }
                    catch
                    {
                        // If fails, do nothing.
                    }

                    // Notify the error by email
                    //await SendMail("notificaciones@tudominio.com",
                    //    "Error en PopulateFacebookAudienceFunction",
                    //    $"Mensaje: {ex.Message}\nStackTrace:\n{ex.StackTrace}");

                    return new ObjectResult($"Error in PopulateFacebookAudienceFunction: {ex.Message}")
                    {
                        StatusCode = StatusCodes.Status500InternalServerError
                    };
                }
            });            

            return new AcceptedResult();
        }

        /// <summary>
        /// Converts the list of dictionaries into the structure expected by Facebook.
        /// </summary>
        private static List<List<string>> PrepareDataForFacebook(List<Dictionary<string, object>> subChunk)
        {
            var finalList = new List<List<string>>();

            foreach (var customer in subChunk)
            {
                var row = new List<string>
                {
                    customer.ContainsKey("Email1") ? customer["Email1"]?.ToString() : "",
                    customer.ContainsKey("Email2") ? customer["Email2"]?.ToString() : "",
                    customer.ContainsKey("Email3") ? customer["Email3"]?.ToString() : "",
                    customer.ContainsKey("Phone1") ? customer["Phone1"]?.ToString() : "",
                    customer.ContainsKey("Phone2") ? customer["Phone2"]?.ToString() : "",
                    customer.ContainsKey("Phone3") ? customer["Phone3"]?.ToString() : "",
                    customer.ContainsKey("FirstName") ? customer["FirstName"]?.ToString() : "",
                    customer.ContainsKey("LastName") ? customer["LastName"]?.ToString() : "",
                    customer.ContainsKey("Zip") ? customer["Zip"]?.ToString() : "",
                    customer.ContainsKey("City") ? customer["City"]?.ToString() : "",
                    customer.ContainsKey("State") ? customer["State"]?.ToString() : "",
                    customer.ContainsKey("Country") ? customer["Country"]?.ToString() : "",
                    customer.ContainsKey("DOBYear") ? customer["DOBYear"]?.ToString() : "",
                    customer.ContainsKey("Gender") ? customer["Gender"]?.ToString() : "",
                };

                finalList.Add(row);
            }

            return finalList;
        }

        /// <summary>
        /// Function to send email.
        /// </summary>
        private static Task SendMail(string recipient, string subject, string body)
        {            
            Console.WriteLine("=== ENVIANDO CORREO ===");
            Console.WriteLine($"To: {recipient}");
            Console.WriteLine($"Subject: {subject}");
            Console.WriteLine($"Body:\n{body}");
            // TO-DO: Implement the email sending logic here.
            return Task.CompletedTask;
        }

        /// <summary>
        /// Queue to Populate FB audiences.
        /// </summary>
        [FunctionName("PopulateFacebookAudienceQueue")]
        public static async Task RunQueue(
            [QueueTrigger("populate-queue", Connection = "AzureWebJobsStorage")]
            string message,
            ILogger log)
        {
            log.LogInformation("===== PopulateFacebookAudienceFunction START =====");

            var payload = JsonConvert.DeserializeObject<PopulateAudiencePayload>(message);

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
    }

    /// <summary>
    /// Model for receiving parameters in the Function
    /// </summary>
    public class PopulateAudiencePayload
    {
        public string AudienceId { get; set; }
        public string FacebookAccessToken { get; set; }

        // Name of the container where the blobs were saved
        public string ContainerName { get; set; }

        // List of blobs (paths) associated with the audience
        public List<string> BlobPaths { get; set; }
    }
}

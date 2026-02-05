using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq; // Needed for JObject parsing if direct deserialization fails

namespace Trigger
{
    // Define response models directly here or reference them if defined elsewhere
    public class AudienceStatusResponse
    {
        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("description")]
        public string Description { get; set; }

        [JsonProperty("approximate_count_lower_bound")]
        public long? ApproximateCountLowerBound { get; set; }

        [JsonProperty("approximate_count_upper_bound")]
        public long? ApproximateCountUpperBound { get; set; }

        [JsonProperty("delivery_status")]
        public DeliveryStatus DeliveryStatus { get; set; }
    }

    public class DeliveryStatus
    {
        [JsonProperty("code")]
        public int Code { get; set; }

        [JsonProperty("description")]
        public string Description { get; set; }
    }

    // Payload for the status check queue
    public class StatusCheckPayload
    {
        public string AudienceId { get; set; }
        public string AudienceName { get; set; }
        public string UserEmail { get; set; }
        public int ExpectedSize { get; set; } // Calculated size
    }


    public static class FBAudienceStatus
    {
        // Use a static HttpClient instance for performance reasons
        private static readonly HttpClient httpClient = new HttpClient();

        // Flag to enable/disable always sending status email (for debugging)
        // Set to true to always send, false to only send on alerts.
        private const bool AlwaysNotifyStatus = true; // <<< SET TO true FOR DEBUGGING

        [FunctionName("CheckAudienceStatusQueue")]
        public static async Task RunQueue(
            [QueueTrigger("status-check-queue", Connection = "AzureWebJobsStorage")]
            string message,
            ILogger log)
        {
            log.LogInformation($"===== CheckAudienceStatusFunction START =====");

            // Decode message from Base64 if needed
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

            StatusCheckPayload payload = null;
            try
            {
                payload = JsonConvert.DeserializeObject<StatusCheckPayload>(jsonMessage);
                if (payload == null || string.IsNullOrEmpty(payload.AudienceId))
                {
                    log.LogError("Invalid payload received in status-check-queue. Missing AudienceId.");
                    throw new ArgumentException("Payload is null or missing AudienceId");
                }

                log.LogInformation($"Checking status for Audience ID: {payload.AudienceId}, Name: {payload.AudienceName}, Expected Size: {payload.ExpectedSize}");

                // --- Call the API Endpoint ---
                // Ensure the base URL is correct and ideally configurable
                string apiBaseUrl = Environment.GetEnvironmentVariable("ApiStatusCheckBaseUrl") ?? "https://omgprod.azurewebsites.net";
                string apiUrl = $"{apiBaseUrl}/audiences/{payload.AudienceId}/status";
                AudienceStatusResponse audienceStatus = null;
                string apiErrorDetails = null;
                bool apiCallSucceeded = false;

                try
                {
                    // Use the static HttpClient instance
                    HttpResponseMessage response = await httpClient.GetAsync(apiUrl);

                    if (response.IsSuccessStatusCode)
                    {
                        string jsonContent = await response.Content.ReadAsStringAsync();
                        log.LogInformation($"API Response: {jsonContent}");
                        audienceStatus = JsonConvert.DeserializeObject<AudienceStatusResponse>(jsonContent);
                        apiCallSucceeded = true;
                    }
                    else
                    {
                        apiErrorDetails = await response.Content.ReadAsStringAsync();
                        log.LogError($"API call to {apiUrl} failed with status {response.StatusCode}. Details: {apiErrorDetails}");
                        // Check for specific error indicating audience not found (e.g., 404)
                        if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                        {
                            await SendAlertEmail(payload, "Audience Not Found", $"The status check API could not find Audience ID {payload.AudienceId}. It might have been deleted or never fully created.", log);
                            return; // Stop processing if audience doesn't exist
                        }
                        // Other errors might be transient, consider retry logic or just alert
                    }
                }
                catch (Exception apiEx)
                {
                    log.LogError(apiEx, $"Exception during API call to {apiUrl}.");
                    apiErrorDetails = apiEx.Message;
                    // Decide if an alert should be sent for API call failure
                    await SendAlertEmail(payload, "Status Check API Error", $"Could not check status for Audience ID {payload.AudienceId}. Error calling API: {apiEx.Message}", log);
                    return; // Stop if API call fails
                }

                // --- Evaluate Status and Send Alerts ---
                if (apiCallSucceeded && audienceStatus != null)
                {
                    bool alertSent = false;
                    long lowerBound = audienceStatus.ApproximateCountLowerBound ?? -1; // Use -1 if null
                    long upperBound = audienceStatus.ApproximateCountUpperBound ?? -1; // Use -1 if null

                    // Case 1: Audience size too low (likely 0 matches)
                    if (lowerBound <= 1000 && upperBound <= 1000 && upperBound != -1) // check upperBound != -1 to ensure data was received
                    {
                        string reason = "Low Estimated Size";
                        string details = $"Estimated audience size is very low ({lowerBound}-{upperBound}), suggesting few or no matches found by Facebook. Expected ~{payload.ExpectedSize}. Delivery Status: {audienceStatus.DeliveryStatus?.Code} - {audienceStatus.DeliveryStatus?.Description}";
                        await SendAlertEmail(payload, reason, details, log);
                        alertSent = true;
                    }
                    // Case 2: Poor match rate (< 10% of expected)
                    // Ensure ExpectedSize is > 0 to avoid division by zero and ensure upperBound is valid
                    else if (payload.ExpectedSize > 0 && upperBound != -1 && upperBound <= (payload.ExpectedSize * 0.10))
                    {
                        double matchPercentage = (double)upperBound / payload.ExpectedSize * 100;
                        string reason = "Poor Match Rate";
                        string details = $"Estimated audience size upper bound ({upperBound}) is less than 10% of the expected size ({payload.ExpectedSize}). Actual match rate: {matchPercentage:F1}%. Delivery Status: {audienceStatus.DeliveryStatus?.Code} - {audienceStatus.DeliveryStatus?.Description}";
                        await SendAlertEmail(payload, reason, details, log);
                        alertSent = true;
                    }
                    // Case 3: Audience Invalid/Not Found (Handled partly by API call check, but delivery_status can also indicate issues)
                    // Example: Code 300 - Audience Too Small, 400 - Expired, etc.
                    else if (audienceStatus.DeliveryStatus != null && audienceStatus.DeliveryStatus.Code != 200) // 200 is 'Ready'
                    {
                        string reason = $"Audience Not Ready (Status Code: {audienceStatus.DeliveryStatus.Code})";
                        string details = $"Audience delivery status indicates it's not ready: '{audienceStatus.DeliveryStatus.Description}'. Expected Size: ~{payload.ExpectedSize}, Actual Size: {lowerBound}-{upperBound}.";
                        await SendAlertEmail(payload, reason, details, log);
                        alertSent = true;
                    }


                    // Case 4: Always Notify (if enabled) and no other alert was sent
                    if (AlwaysNotifyStatus && !alertSent)
                    {
                        string reason = "Status Update";
                        string details = $"Audience ID: {audienceStatus.Id}\n" +
                                         $"Name: {audienceStatus.Name}\n" +
                                         $"Description: {audienceStatus.Description}\n" +
                                         $"Expected Size: ~{payload.ExpectedSize}\n" +
                                         $"Estimated Size: {lowerBound} - {upperBound}\n" +
                                         $"Delivery Status Code: {audienceStatus.DeliveryStatus?.Code}\n" +
                                         $"Delivery Status Desc: {audienceStatus.DeliveryStatus?.Description}";
                        await helper.SendMail(payload.UserEmail, $"Facebook Audience Status: {payload.AudienceName}", details);
                        log.LogInformation("Sent standard status update email (AlwaysNotify=true).");
                    }
                    else if (!alertSent)
                    {
                        log.LogInformation($"Audience {payload.AudienceId} status check passed. Estimated size: {lowerBound}-{upperBound}. Expected: {payload.ExpectedSize}. Status: {audienceStatus.DeliveryStatus?.Code}");
                    }
                }
                else if (apiCallSucceeded && audienceStatus == null) // Should not happen if API returns 200, but check just in case
                {
                    log.LogError($"API call succeeded but failed to deserialize response for Audience ID {payload.AudienceId}.");
                    await SendAlertEmail(payload, "Status Check Internal Error", $"Could not process the status response for Audience ID {payload.AudienceId} even though the API call was successful.", log);
                }
                // Case where API call failed is handled within the API call block

            }
            catch (JsonException jsonEx)
            {
                log.LogError(jsonEx, "Failed to deserialize message from status-check-queue.");
                // Cannot proceed without payload, message will likely poison after retries
            }
            catch (Exception ex)
            {
                log.LogError(ex, "An unexpected error occurred in CheckAudienceStatusFunction.");
                // Send generic error email if payload was parsed
                if (payload != null)
                {
                    await helper.SendMail(payload.UserEmail,
                        $"Error in Audience Status Check for {payload.AudienceName}",
                        $"An unexpected error occurred while checking the status for Audience ID {payload.AudienceId}.\nError: {ex.Message}\nStackTrace:\n{ex.StackTrace}");
                }
                // Rethrow to let the Functions runtime handle retries as configured
                throw;
            }
            finally
            {
                log.LogInformation("===== CheckAudienceStatusFunction END =====");
            }
        }

        // Helper method to standardize alert email sending
        private static async Task SendAlertEmail(StatusCheckPayload payload, string reason, string details, ILogger log)
        {
            string subject = $"ALERT: Facebook Audience Issue ({reason}) - {payload.AudienceName}";
            string body = $"An issue was detected for Facebook Custom Audience:\n\n" +
                          $"Audience ID: {payload.AudienceId}\n" +
                          $"Audience Name: {payload.AudienceName}\n\n" +
                          $"Reason: {reason}\n\n" +
                          $"Details:\n{details}\n\n" +
                          $"Please investigate.";

            await helper.SendMail(payload.UserEmail, subject, body);
            log.LogWarning($"Alert email sent to {payload.UserEmail} for Audience ID {payload.AudienceId}. Reason: {reason}");
        }
    }
}
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using static System.Net.WebRequestMethods;
using static Trigger.Function1;

namespace Trigger
{
     public static class Function1
     {
          public class Log
          {
               public Log(string _Message)
               {
                    Message = _Message;
               }
               public string Message { get; set; }
          }

          [FunctionName("Function1")]
          public static async Task Run([TimerTrigger("0 0 8 * * *")] TimerInfo myTimer, ILogger log)
          {
               try
               {
			     log.LogInformation($"Upload process starts at {DateTime.Now.ToString("hh:mm:ss")}...");
			     HttpClient client = new HttpClient();

			     string[] baseUrls = new string[] { "https://omgdev.azurewebsites.net/", "https://omgprod.azurewebsites.net/" };
			     string logsUrl = "https://omgdev.azurewebsites.net/api/Logs/Create";

                    foreach (var baseUrl in baseUrls)
                    {
                         var urls = new Dictionary<string, string>();
			          urls.Add("GET", $"{baseUrl}DataFiles/UpdateHoldRecords");
			          urls.Add("POST", $"{baseUrl}DataFiles/DataFileUpload");

			          foreach (var url in urls)
			          {
                              HttpResponseMessage response = url.Key == "GET" ? await client.GetAsync(url.Value) : await client.PostAsJsonAsync(url.Value, new{});

                              if (response.StatusCode != HttpStatusCode.OK)
                              {
					          log.LogError($"There was an error on the call");
				          }
                              else
                              {
                                   var stringRes = await response.Content.ReadAsStringAsync();
                                   string logMessage = $"URL {url} called with result: {stringRes}";
				               await client.PostAsJsonAsync(logsUrl, new Log(logMessage));
                                   log.LogInformation(logMessage);
                              }
			          }
                    }
               }
               catch (Exception ex)
               {
				log.LogError(ex.Message);
               }

		}
     }
}

using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System;
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
          public static async Task Run([TimerTrigger("0 0 15 * * *")] TimerInfo myTimer, ILogger log)
          {

			log.LogInformation($"Upload process starts at {DateTime.Now.ToString("hh:mm:ss")}...");
			HttpClient client = new HttpClient();

			string logsUrl = "https://omgdev.azurewebsites.net/api/Logs/Create";

			string[] urls = new string[]{
				$"https://omgdev.azurewebsites.net/DataFiles/UpdateHoldRecords",
				$"https://omgdev.azurewebsites.net/DataFiles/DataFileUpload"
			};

               foreach (var url in urls)
			{
				log.LogInformation($"Calling URL {url} at {DateTime.Now.ToString("hh:mm:ss")}...");
				HttpResponseMessage response = await client.GetAsync(url);
                    var stringRes = await response.Content.ReadAsStringAsync();

                    string logMessage = $"URL {url} called with result: {stringRes}";
				await client.PostAsJsonAsync(logsUrl, new Log(logMessage));
                    log.LogInformation(logMessage);
			}
		}
     }
}

//   
//   Copyright © Microsoft Corporation, All Rights Reserved
// 
//   Licensed under the Apache License, Version 2.0 (the "License"); 
//   you may not use this file except in compliance with the License. 
//   You may obtain a copy of the License at
// 
//   http://www.apache.org/licenses/LICENSE-2.0 
// 
//   THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS
//   OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION
//   ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A
//   PARTICULAR PURPOSE, MERCHANTABILITY OR NON-INFRINGEMENT.
// 
//   See the Apache License, Version 2.0 for the specific language
//   governing permissions and limitations under the License. 

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Demo.Azure.MessageSessions
{
    using System;
    using System.IO;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.ServiceBus;
    using Microsoft.Azure.ServiceBus.Core;
    using Newtonsoft.Json;

    public class Program
    {
        public const string SessionQueueName = "SessionQueue";
        public const string ServiceBusConnectionStringKey = "ServiceBusConnectionStringKey";
        static readonly string samplePropertiesFileName = "azure-msg-config.properties";

        public async Task Run(string connectionString)
        {
            Console.WriteLine("Press any key to exit the scenario");

            await Task.WhenAll(
                this.SendMessagesAsync("Client1", connectionString, SessionQueueName),
                this.SendMessagesAsync("Client2", connectionString, SessionQueueName),
                this.SendMessagesAsync("Client3", connectionString, SessionQueueName),
                this.SendMessagesAsync("Client4", connectionString, SessionQueueName));

            var listener1 = this.InitializeReceiver(connectionString, SessionQueueName, "Listener1");
            var listener2 = this.InitializeReceiver(connectionString, SessionQueueName, "Listener2");
            var listener3 = this.InitializeReceiver(connectionString, SessionQueueName, "Listener3");

            Task.WaitAny(
               Task.Run(() => Console.ReadKey()),
               Task.Delay(TimeSpan.FromSeconds(10)));

            await listener1.CloseAsync();
            await listener2.CloseAsync();
            await listener3.CloseAsync();
        }

        async Task SendMessagesAsync(string sessionId, string connectionString, string queueName)
        {
            var sender = new MessageSender(connectionString, queueName);

            dynamic data = new[]
            {
                new {step = 1, title = "Shop"},
                new {step = 2, title = "Unpack"},
                new {step = 3, title = "Prepare"},
                new {step = 4, title = "Cook"},
                new {step = 5, title = "Eat"},
            };

            for (int i = 0; i < data.Length; i++)
            {
                var message = new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i])))
                {
                    SessionId = sessionId,
                    ContentType = "application/json",
                    Label = "RecipeStep",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };
                await sender.SendAsync(message);
                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Session {0}, PayrunId = {1}", message.SessionId, message.MessageId);
                    Console.ResetColor();
                }
            }
        }

        QueueClient InitializeReceiver(string connectionString, string queueName, string name)
        {
            var client = new QueueClient(connectionString, queueName, ReceiveMode.PeekLock);
            client.RegisterSessionHandler(
               async (session, message, cancellationToken) =>
               {
                   if (message.Label != null &&
                       message.ContentType != null &&
                       message.Label.Equals("RecipeStep", StringComparison.InvariantCultureIgnoreCase) &&
                       message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
                   {
                       var body = message.Body;

                       dynamic recipeStep = JsonConvert.DeserializeObject(Encoding.UTF8.GetString(body));
                       lock (Console.Out)
                       {
                           Console.ForegroundColor = ConsoleColor.Cyan;
                           Console.WriteLine(
                               "\t\t\t\tMessage received:  \n\t\t\t\t\t\tName = {0}, \n\t\t\t\t\t\tSessionId = {1}, \n\t\t\t\t\t\tPayrunId = {2}, \n\t\t\t\t\t\tSequenceNumber = {3}," +
                               "\n\t\t\t\t\t\tContent: [ step = {4}, title = {5} ]",
                               name,
                               message.SessionId,
                               message.MessageId,
                               message.SystemProperties.SequenceNumber,
                               recipeStep.step,
                               recipeStep.title);
                           Console.ResetColor();
                       }
                       await session.CompleteAsync(message.SystemProperties.LockToken);

                       if (recipeStep.step == 5)
                       {
                           // end of the session!
                           await session.CloseAsync();
                       }
                   }
                   else
                   {
                       await session.DeadLetterAsync(message.SystemProperties.LockToken);//, "BadMessage", "Unexpected message");
                   }
               },
                new SessionHandlerOptions(LogMessageHandlerException)
                {
                    MessageWaitTimeout = TimeSpan.FromSeconds(5),
                    MaxConcurrentSessions = 1,
                    AutoComplete = false
                });
            return client;
        }

        private Task LogMessageHandlerException(ExceptionReceivedEventArgs e)
        {
            Console.WriteLine("Exception: \"{0}\" {1}", e.Exception.Message, e.ExceptionReceivedContext.EntityPath);
            return Task.CompletedTask;
        }

        [DebuggerStepThrough]
        public void RunSample(string[] args, Func<string, Task> run)
        {
            var properties = new Dictionary<string, string>
            {
                {ServiceBusConnectionStringKey, "Endpoint=sb://paycor-azmonitor-poc.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=eAnKbrZdVULtTO1ItrbvI5LrE+vcSvbMuagr61Nt3ng="},
            };

            // read the settings file created by the ./setup.ps1 file
            var settingsFile = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                samplePropertiesFileName);
            if (File.Exists(settingsFile))
            {
                using (var fs = new StreamReader(settingsFile))
                {
                    while (!fs.EndOfStream)
                    {
                        var readLine = fs.ReadLine();
                        if (readLine != null)
                        {
                            var propl = readLine.Trim();
                            var cmt = propl.IndexOf('#');
                            if (cmt > -1)
                            {
                                propl = propl.Substring(0, cmt).Trim();
                            }
                            if (propl.Length > 0)
                            {
                                var propi = propl.IndexOf('=');
                                if (propi == -1)
                                {
                                    continue;
                                }
                                var propKey = propl.Substring(0, propi).Trim();
                                var propVal = propl.Substring(propi + 1).Trim();
                                if (properties.ContainsKey(propKey))
                                {
                                    properties[propKey] = propVal;
                                }
                            }
                        }
                    }
                }
            }

            // get overrides from the environment
            foreach (var prop in properties.Keys.ToArray())
            {
                var env = Environment.GetEnvironmentVariable(prop);
                if (env != null)
                {
                    properties[prop] = env;
                }
            }

            run(properties[ServiceBusConnectionStringKey]).GetAwaiter().GetResult();
        }

        public static int Main(string[] args)
        {
            try
            {
                var app = new Program();
                app.RunSample(args, app.Run);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
                return 1;
            }
            return 0;
        }
    }
}
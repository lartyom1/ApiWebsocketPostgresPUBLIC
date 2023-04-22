using System;
using System.Threading;

using System.Net.Http;
using Websocket.Client;

using Newtonsoft.Json;
using Npgsql;

using NLog;
using System.Configuration;
using System.Diagnostics;

using System.Drawing;
using System.Globalization;

namespace console_websocket
{
    class Program
    {
        string BasicAuth = ConfigurationManager.AppSettings["BasicAuth"];
        string UrlAuth = ConfigurationManager.AppSettings["UrlAuth"];
        string WsUri = ConfigurationManager.AppSettings["WsUri"];
        string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
        string AlphasUri = ConfigurationManager.AppSettings["AlphasUri"];
        string AlphaImgUri = ConfigurationManager.AppSettings["AlphaImgUri"];
        string Path = ConfigurationManager.AppSettings["Path"];

        public List<String> Subscriptions = new List<string>(ConfigurationManager.AppSettings["Subscriptions"].Split(new char[] { ';' }));

        uint RcvCounter = 0;
        uint SentCounter = 0;

        uint RcvICounter = 0;
        uint SentICounter = 0;

        StateDictionary StateDict = new StateDictionary();
        SourceDictionary SourceDict = new SourceDictionary();
        GammaDictionary GammaDict = new GammaDictionary();

        static HttpClientHandler ClientHandler = new HttpClientHandler();
        HttpClient HttpClient = new HttpClient(ClientHandler);
        HttpRequestMessage request = new HttpRequestMessage();

        Stopwatch sw = Stopwatch.StartNew();
        Logger log = LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {

            if (ConfigurationManager.AppSettings["BasicAuth"] != null)
            {
                Program prg = new Program();
                prg.Initialize().Wait();
            }
            else
            {
                ColorLine("config file missing", ConsoleColor.Red);
            }
        }

        async Task Initialize()
        {
            Console.WriteLine("subscriptions:");
            foreach (var item in Subscriptions)
            {
                Console.WriteLine(item);
            }

            Console.WriteLine();

            sw.Start();
            log.Info("--------------------------------------");
            log.Info("Script startet at: " + DateTime.Now);
            log.Info("--------------------------------------");

            Console.CursorVisible = false;

            ClientHandler.ServerCertificateCustomValidationCallback = (sender, cert, chain, sslPolicyErrors) => { return true; };
            HttpClient.DefaultRequestHeaders.Add("Authorization", BasicAuth);
            request.Method = HttpMethod.Get;

            //Pg server is too busy from xx:00 to xx:16
            if (DateTime.Now.Minute <= 15)
            {
                ColorLine("Wait for " + (15 - DateTime.Now.Minute + 1) + " minutes \n", ConsoleColor.DarkGreen);
                Task.Delay(TimeSpan.FromMinutes(15 - DateTime.Now.Minute + 1)).ContinueWith(t => UpdateAlphas());
            }

            else if (DateTime.Now.Minute >= 17)
            {
                Task.Delay(TimeSpan.FromMinutes(60 - (DateTime.Now.Minute - 16))).ContinueWith(t => UpdateAlphas());
                ColorLine("Wait for " + (60 - (DateTime.Now.Minute - 16)) + " minutes \n", ConsoleColor.DarkGreen);
            }
            else
            {
                UpdateAlphas();
            }

            await GetStateDictionary();
            await GetSourceDictionary();
            await GetGammaDictionary();

            var token = await GetToken();
            WsConnect(token.ToString());
        }

        static void ColorLine(string text, ConsoleColor color)
        {
            Console.ForegroundColor = color;
            Console.WriteLine(text);
            Console.ForegroundColor = ConsoleColor.White;
        }

        int attempts = 0;
        async Task<string> GetToken()
        {
            if (attempts > 0)
            {
                if (attempts > 10)
                {
                    Console.WriteLine("too many attempts, press any key to continue");
                    Console.ReadKey();
                }
                Console.WriteLine("retrying to get token in " + attempts + " sec");
                Thread.Sleep(1000 * attempts);
            }

            var x = await HttpClient.GetAsync(UrlAuth);
            string json = await x.Content.ReadAsStringAsync();

            string status = x.StatusCode.ToString();

            attempts++;

            if (status == "Unauthorized")
            {
                Console.WriteLine("UNAUTHORIZED token request");
            }

            else
            {
                if (x.Content is object && x.Content.Headers.ContentType.MediaType == "application/json")
                {
                    var res = JsonConvert.DeserializeObject<Token>(json);
                    string key = res.Data.Token;
                    Console.WriteLine("token accuired: " + key);

                    log.Info("token accuired: " + key);
                    return key;
                }
            }
            return "";
        }

        async Task GetStateDictionary()//Alpha states dictionary
        {
            using var dataSource = NpgsqlDataSource.Create(ConnectionString);

            await using var command = dataSource.CreateCommand("SELECT * FROM \"Schema_1\".\"StatesDictionary\"");
            await using var reader = await command.ExecuteReaderAsync();

            StateDict.ListData = new List<DictionaryLine>();
            while (await reader.ReadAsync())
            {
                StateDict.ListData.Add(new DictionaryLine()
                {
                    StateId = reader.GetInt32(0),
                    FieldOne = reader.GetString(1),
                    /*
                     * --- *
                     * --- *
                     */ 
                    FieldTwo = reader.GetBoolean(2),
                    FieldThree = reader.GetBoolean(3)
                });
            }
            Console.WriteLine("state dictionary:");
            foreach (var item in StateDict.ListData)
            {
                Console.WriteLine(item.StateId + " " + item.FieldOne + " " + item.FieldTwo + " " + item.FieldThree);
            }
            Console.WriteLine("");
        }

        async Task GetSourceDictionary()
        {
            using var dataSource = NpgsqlDataSource.Create(ConnectionString);

            await using var command = dataSource.CreateCommand("SELECT * FROM \"Schema_1\".\"SourceName\"");
            await using var reader = await command.ExecuteReaderAsync();

            SourceDict.ListData = new List<SourceLine>();
            while (await reader.ReadAsync())
            {
                SourceDict.ListData.Add(new SourceLine()
                {
                    SourceId = reader.GetInt32(0),
                    SourceName = reader.GetString(1)
                });
            }
            Console.WriteLine("sources:");
            foreach (var item in SourceDict.ListData)
            {
                Console.WriteLine(item.SourceId + " " + item.SourceName);
            }
            Console.WriteLine("");
        }

        async Task GetGammaDictionary()
        {
            using var dataSource = NpgsqlDataSource.Create(ConnectionString);

            await using var command = dataSource.CreateCommand("SELECT * FROM \"Schema_1\".\"GammaType\" order by \"IdGamma\" asc");
            await using var reader = await command.ExecuteReaderAsync();

            GammaDict.ListData = new List<GammaLine>();
            while (await reader.ReadAsync())
            {
                GammaDict.ListData.Add(new GammaLine()
                {
                    IdGamma = reader.GetInt32(0),
                    Description = reader.GetString(1)
                });
            }
            Console.WriteLine("Gamma event types:");
            foreach (var item in GammaDict.ListData)
            {
                Console.WriteLine(item.IdGamma + " " + item.Description);
            }
            Console.WriteLine("");
        }

        async Task<string> GetAlphas()
        {
            var x = await HttpClient.GetAsync(AlphasUri);

            var json = x.Content.ReadAsStringAsync().Result;

            string status = x.StatusCode.ToString();

            if (status == "Unauthorized")
            {
                Console.WriteLine("UNAUTHORIZED");
            }

            else
            {
                if (x.Content is object && x.Content.Headers.ContentType.MediaType == "application/json")
                {
                    return json.ToString();
                }
            }
            return null;
        }

        async Task UpdateAlphas()
        {
            Task.Delay(TimeSpan.FromHours(1)).ContinueWith(t => UpdateAlphas());

            string alphasJson = await GetAlphas();

            if (alphasJson != null)
            {
                string requestHead = "INSERT INTO \"Schema_1\".Alpha(\"IdAlpha\", \"Server\", \"Name\", \"Url\") VALUES ";
                string requestValues = "";

                //Alpha's parameters may change over time, they should be uppdated every hour
                string requestConflict = " on conflict(\"IdAlpha\") do update set \"Server\" = EXCLUDED.\"Server\", \"Name\" = EXCLUDED.\"Name\"," +
                                           "\"Url\" = EXCLUDED.\"Url\"";
                //string requestConflict = " on conflict(\"IdAlpha\") do NOTHING"; \\ in case there is no need to update existent Alphas, only new will be added

                //It is very important to know whether alpha is enabled, every hour alpha enabled is logged into db
                string requestHead2 = "INSERT INTO \"Schema_1\".\"AlphaEnabled\"(\"IdAlpha\", \"Enabled\", \"Date\") VALUES ";
                string requestValues2 = "";


                var alphas = JsonConvert.DeserializeObject<Root>(alphasJson);

                foreach (var alpha in alphas.Data)
                {
                    requestValues += $"({alpha.Id}" +
                                  $", '{alpha.Server}', {alpha.Name}, '{alpha.Url}')";

                    requestValues2 += $"({alpha.Id},{alpha.Status.Enabled},'{DateTime.Now}')";

                    if (alphas.Data.IndexOf(alpha) != alphas.Data.Count - 1)
                    {
                        requestValues += ",\r\n";
                        requestValues2 += ",\r\n";
                    }
                }

                try
                {
                    using var dataSource = NpgsqlDataSource.Create(ConnectionString);

                    string request = requestHead + requestValues + requestConflict;
                    using var command = dataSource.CreateCommand(request);
                    await command.ExecuteNonQueryAsync();

                    using var dataSource2 = NpgsqlDataSource.Create(ConnectionString);

                    string request2 = requestHead2 + requestValues2;
                    using var command2 = dataSource2.CreateCommand(request2);
                    await command2.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    ColorLine(ex.ToString(), ConsoleColor.Red);
                }

                //Console.WriteLine(request);
            }
            ColorLine(DateTime.Now + "UpdateAlphas \n", ConsoleColor.DarkGreen);
            log.Info("UpdateAlphas");

        }

        async Task PushPgStatesReduced(string idAlpha, string idState, DateTime dt, int attempt)
        {
            try
            {
                using var dataSource = NpgsqlDataSource.Create(ConnectionString);
                using var command = dataSource.CreateCommand("INSERT INTO \"Schema_1\".\"AlphaState\" (\"IdAlpha\",\"IdState\",\"Date\")" +
                    " VALUES ('" + idAlpha + "','" + idState + "','" + dt + "." + dt.Millisecond + "') ");

                await command.ExecuteNonQueryAsync();

                if (attempt > 1)
                {
                    ColorLine("RePush event (Alpha state)R dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt, ConsoleColor.Magenta);
                    log.Info("RePush event (Alpha state)R dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt);
                }
                SentCounter++;
            }

            catch (Exception ex)
            {
                // in case of an error data is pushed again with one minute delay, recursively
                ColorLine("ERROR: " + ex.ToString() + "\n", ConsoleColor.Red);
                ColorLine("failed to push event (Alpha state)R dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt, ConsoleColor.Red);

                //log.Error(ex.ToString());

                log.Error("");
                log.Error("failed to push event (Alpha state)R dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt);
                log.Error("INSERT INTO \"Schema_1\".\"AlphaState\" (\"IdAlpha\",\"IdState\",\"Date\")" +
                    " VALUES ('" + idAlpha + "','" + idState + "','" + dt + "." + dt.Millisecond + "') ");

                attempt++;

                if (!ex.ToString().Contains(("23505:"))) // websocket may send dupes sometimes, ignore them  //(0x80004005): 23505
                {
                    Task.Delay(60000).ContinueWith(t => PushPgStatesReduced(idAlpha, idState, dt, attempt));
                }
                else
                {
                    ColorLine("Dupe Ignored", ConsoleColor.DarkYellow);
                    log.Error("Dupe Ignored");
                }

            }
        }
        
        async Task PushPgGamma(string idGamma, string source, string comment, DateTime dt, string alphaId, string visualisation, int attempt)
        {
            try
            {
                using var dataSource = NpgsqlDataSource.Create(ConnectionString);

                using var command = dataSource.CreateCommand("INSERT INTO \"Schema_1\".\"Gamma\" (\"IdGamma\",\"Source\",\"Date\",\"IdAlpha\")" +
              " VALUES ('" + idGamma + "','" + source + "','" + dt + "." + dt.Millisecond + "','" + alphaId + "') ");  //"','" + focused +

                await command.ExecuteNonQueryAsync();

                if (attempt > 1)
                {
                    ColorLine("RePush event Gamma dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt, ConsoleColor.Magenta);
                    log.Info("RePush event Gamma dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt);
                }
                SentICounter++;
            }

            catch (Exception ex)
            {
                // in case of an error data is pushed again with one minute delay, recursively
                ColorLine("ERROR: " + ex.ToString() + "\n", ConsoleColor.Red);
                ColorLine("failed to push Gamma dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt, ConsoleColor.Red);

                //log.Error(ex.ToString());

                log.Error("");
                log.Error("failed to push Gamma dt: " + dt + "." + dt.Millisecond + ", attempt: " + attempt);
                log.Error("INSERT INTO \"Schema_1\".\"Gamma\" (\"IdGamma\",\"Source\",\"Date\",\"IdAlpha\")" +
                " VALUES ('" + idGamma + "','" + source + "','" + dt + "." + dt.Millisecond + "','" + alphaId + "') " + comment);

                attempt++;

                if (!ex.ToString().Contains(("23505:"))) // websocket may send dupes sometimes, ignore them  //(0x80004005): 23505
                {
                    Task.Delay(60000).ContinueWith(t => PushPgGamma(idGamma, source, comment, dt, alphaId, visualisation, attempt));
                }
                else
                {
                    ColorLine("Dupe Ignored", ConsoleColor.DarkYellow);
                    log.Error("Dupe Ignored");
                }
            }
        }
        
        async Task PushStateDictionaryLine(string parameterOne, string stateOne, string stateTwo, string stateThree, int attempt)
        {
            //State Dictionary Id is auto increment
            try
            {
                using var dataSource = NpgsqlDataSource.Create(ConnectionString);

                using var command = dataSource.CreateCommand("INSERT INTO \"Schema_1\".\"StatesDictionary\" (\"ParameterOne\",\"StateOne\",\"StateTwo\",\"StateThree\")" +
                    " VALUES ('" + parameterOne + "','" + stateOne + "','" + stateTwo + "','" + stateThree + "') ");

                await command.ExecuteNonQueryAsync();

                if (attempt > 1)
                {
                    ColorLine("RePush dictionary line, attempt: " + attempt, ConsoleColor.Magenta);
                    log.Info("RePush dictionary line, attempt: " + attempt);
                }
            }

            catch (Exception ex)
            {
                // in case of an error data is pushed again with one minute delay, recursively
                ColorLine("ERROR: " + ex.ToString() + "\n", ConsoleColor.Red);
                ColorLine("failed to add dictionary line, attempt: " + attempt, ConsoleColor.Red);

                log.Error("failed to add dictionary line, attempt: " + attempt);
                //log.Error(ex.ToString());

                attempt++;
                Task.Delay(60000).ContinueWith(t => PushStateDictionaryLine(parameterOne, stateOne, stateTwo, stateThree, attempt));
            }
        }

        async Task PushSourceDictionaryLine(string sourceName, int attempt)
        {
            //Source Dictionary Id is autoo increment
            try
            {
                using var dataSource = NpgsqlDataSource.Create(ConnectionString);

                using var command = dataSource.CreateCommand("INSERT INTO \"Schema_1\".\"SourceName\" (Name)" +
                    " VALUES ('" + sourceName + "') ");

                await command.ExecuteNonQueryAsync();

                if (attempt > 1)
                {
                    ColorLine("RePush source dictionary line, attempt: " + attempt, ConsoleColor.Magenta);
                    log.Info("RePush source dictionary line, attempt: " + attempt);
                }
            }

            catch (Exception ex)
            {
                // in case of an error data is pushed again with one minute delay, recursively
                ColorLine("ERROR: " + ex.ToString() + "\n", ConsoleColor.Red);
                ColorLine("failed to add source dictionary line, attempt: " + attempt, ConsoleColor.Red);

                log.Error("failed to add source dictionary line, attempt: " + attempt);
                //log.Error(ex.ToString());

                attempt++;
                Task.Delay(60000).ContinueWith(t => PushSourceDictionaryLine(sourceName, attempt));
            }
        }

        async Task SaveImg(EventGamma gamma, string alphaId, string visualisation, string sourceId, string gammaId)
        {

            string req = $"{AlphaImgUri}{alphaId}/image/"
                                + $"{gamma.Data.Time.Year}{gamma.Data.Time.Month:00}{gamma.Data.Time.Day:00}T"
                                + $"{gamma.Data.Time.Hour:00}{gamma.Data.Time.Minute:00}{gamma.Data.Time.Second:00}" +
                                "." + gamma.Data.Time.Millisecond; // image request from api

            Image img = await GetImg(req);

            if (img != null)
            {
                Bitmap bmp = AddRectangle(img, visualisation);
                
                //YEAR-MONTH-DAY folder and subfolders
                string folder = string.Format("{0}\\{0}_{1:00}\\{0}_{1:00}_{2:00}", gamma.Data.Time.Year, gamma.Data.Time.Month, gamma.Data.Time.Day);
                Directory.CreateDirectory(Path + folder);

                //all necessary info is coded in filename
                Console.WriteLine(Path + folder);
                Console.WriteLine(Path + folder + "\\" + "gm" + gammaId + ".alpha" + alphaId + ".src" + sourceId + "." + gamma.Data.Time.Year + "_"
                    + $"{gamma.Data.Time.Month:00}_{gamma.Data.Time.Day:00}_{gamma.Data.Time.Hour:00}_{gamma.Data.Time.Minute:00}_{gamma.Data.Time.Second:00}"
                    + $"_000.jpg");

                BitmapExtensions.SaveJPG(bmp, Path + folder + "\\" + "gm" + gammaId + ".alpha" + alphaId + ".src" + sourceId + "." + gamma.Data.Time.Year + "_"
                    + $"{gamma.Data.Time.Month:00}_{gamma.Data.Time.Day:00}_{gamma.Data.Time.Hour:00}_{gamma.Data.Time.Minute:00}_{gamma.Data.Time.Second:00}"
                    + $"_000.jpg");
                 
                ColorLine("Saved", ConsoleColor.Green);
            }
            else
            {
                log.Error("failed to load image, gamma:" + gamma.Data.Time + ", cam: " + alphaId + ", source: " + sourceId);
                ColorLine("failed to load image, gamma:" + gamma.Data.Time + ", cam: " + alphaId + ", source: " + sourceId, ConsoleColor.Red);
            }
        }

        async Task<Image> GetImg(string req)
        {
            using var res = await HttpClient.GetAsync(req);//slow

            byte[] bytes = await res.Content.ReadAsByteArrayAsync();


            if (res.Content is object && res.Content.Headers.ContentType.MediaType == "image/jpeg")
            {
                Image img = Image.FromStream(new MemoryStream(bytes));
                return img;
            }
            else
            {
                log.Error("gamma content" + res.Content.ReadAsStringAsync() + res.ToString() + " req " + req);
                ColorLine("gamma content" + res.Content.ReadAsStringAsync() + res.ToString() + " req " + req, ConsoleColor.Red);
            }

            return null;
        }

        Bitmap AddRectangle(Image img, string parameters)
        {
            // LOCALE DOT-COMMA FOR DOUBLE
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");

            var photo = new Bitmap(img);
            if (parameters != null && parameters.Length > 0)
            {
                string[] substring = parameters.Split(';');

                substring[0] = substring[0].Replace("color:", "");
                substring[1] = substring[1].Replace("rect:", "");

                string[] color = substring[0].Split(',');
                string[] position = substring[1].Split(',');

                int posX = (int)Math.Floor(0.01 * img.Width * Double.Parse(position[0]));
                int posY = (int)Math.Floor(0.01 * img.Height * Double.Parse(position[1]));

                int sizeX = (int)Math.Floor(0.01 * img.Width * Double.Parse(position[2]));
                int sizeY = (int)Math.Floor(0.01 * img.Height * Double.Parse(position[3]));

                var bitmap = new Bitmap(img.Width, img.Height);
                var graphics = Graphics.FromImage(bitmap);

                graphics.DrawImageUnscaled(photo, 0, 0);

                //graphics.DrawString("QuickBrownFox", new Font("Arial", 18), Brushes.Magenta, 0, 400);

                Color colorRGB = new Color();
                colorRGB = Color.FromArgb(255, Int32.Parse(color[0]), Int32.Parse(color[1]), Int32.Parse(color[2]));
                Pen PgbPen = new Pen(colorRGB, 5);
                Rectangle rect = new Rectangle(posX, posY, sizeX, sizeY);

                graphics.DrawRectangle(PgbPen, rect);

                return bitmap;
            }
            return photo;
        }

        private void WsConnect(string key)
        {
            try
            {
                var exitEvent = new ManualResetEvent(false);

                string keyJson = "{\"type\": \"auth\",\"token\":\"" + key + "\"}"; //auth json 
                string getTime = "{\"type\":\"get_server_time\"}";

                using (var client = new WebsocketClient(new Uri(WsUri)))
                {
                    client.ReconnectTimeout = TimeSpan.FromSeconds(60);

                    client.ReconnectionHappened.Subscribe(async info => //token and subscription must be sent again after reconnect
                    {
                        string reconnMsg = info.Type.ToString();

                        ColorLine("\n" + DateTime.Now + " (Re)Connection happened, type: " + info.Type + "\n", ConsoleColor.DarkGreen);
                        log.Info("(Re)Connection happened, type: " + info.Type);

                        if (reconnMsg != "Initial") //prevent authenticating twice on initial connection                    
                        {
                            client.Send(keyJson);

                            foreach (var item in Subscriptions)
                            {
                                client.Send(item);
                            }

                            client.Send(getTime);
                        }
                    });

                    client.MessageReceived.Subscribe(async msg =>
                    {
                        string rcvMsg = msg.ToString();

                        if (rcvMsg.Contains("auth"))
                        {
                            ColorLine("authentication response------------------------------------" +
                                            "-------------------------------------------------------------", ConsoleColor.DarkGreen);
                            ColorLine(DateTime.Now + " Received: " + rcvMsg + "\n", ConsoleColor.DarkGreen);

                            if (rcvMsg.Contains("auth success"))
                            {
                                attempts = 0;
                            }
                            if (rcvMsg.Contains("INVALID"))//ReAuth
                            {
                                var newKey = await GetToken();
                                keyJson = "{\"type\": \"auth\",\"token\":\"" + newKey.ToString() + "\"}";
                                Console.WriteLine("new key: " + newKey.ToString());

                                client.Send(keyJson);

                                foreach (var item in Subscriptions)
                                {
                                    client.Send(item);
                                }

                                client.Send(getTime);
                            }
                        }

                        else if (rcvMsg.Contains("serverTime"))
                        {
                            ColorLine(DateTime.Now + " gettime response-------------------------------------------" +
                                                 "-----------------------------------------", ConsoleColor.DarkCyan);
                            ColorLine("Received: " + rcvMsg + "\n", ConsoleColor.DarkCyan);
                        }

                        else if (rcvMsg.Contains("rt") && rcvMsg.Contains("states")) // states changed                    
                        {
                            var states = JsonConvert.DeserializeObject<AlphaData>(rcvMsg);

                            states.NestedData.Time = DateTime.Parse(states.NestedData.Time.ToString("yyyy-MM-dd HH:mm:ss.ff"));//round millis

                            if (states.NestedData.Param.ParameterOne != null)
                            {
                                RcvCounter++;

                                Console.WriteLine("Alpha states response----------------------------------------" +
                                              "-------------------------------------------------------------");
                                Console.WriteLine(DateTime.Now + " Received: " + rcvMsg);

                                bool identified = false;
                                for (int i = 0; i < StateDict.ListData.Count; i++)
                                {

                                    if (states.NestedData.Param.ParameterOne == StateDict.ListData[i].FieldOne
                                    && states.NestedData.States.StateTwo == StateDict.ListData[i].FieldTwo
                                    && states.NestedData.States.StateThree == StateDict.ListData[i].FieldThree)
                                    {
                                        Console.WriteLine("Alpha state at time " + states.NestedData.Time + "." + states.NestedData.Time.Millisecond + " identified as state " + StateDict.ListData[i].StateId);

                                        PushPgStatesReduced(states.NestedData.Id, StateDict.ListData[i].StateId.ToString(), states.NestedData.Time, 1);
                                        identified = true;
                                        break;
                                    }
                                }

                                if (!identified)
                                {
                                    ColorLine("Alpha state at time " + states.NestedData.Time + " NOT identified", ConsoleColor.Red);
                                    log.Error("failed to identify state dt: " + states.NestedData.Time);

                                    log.Error(states.NestedData.Id + " " + states.NestedData.Param.ParameterOne + " " + states.NestedData.Time + " " + states.NestedData.States.StateOne + " " +
                                              states.NestedData.States.StateTwo + " " + states.NestedData.States.StateThree);

                                    await PushStateDictionaryLine(states.NestedData.Param.ParameterOne,
                                          states.NestedData.States.StateOne.ToString(), states.NestedData.States.StateTwo.ToString(), states.NestedData.States.StateThree.ToString(), 1);

                                    await GetStateDictionary();

                                    Console.WriteLine("undefined message state id must be: " + StateDict.ListData[StateDict.ListData.Count - 1].StateId);// index != count
                                    PushPgStatesReduced(states.NestedData.Id, StateDict.ListData[StateDict.ListData.Count - 1].StateId.ToString(), states.NestedData.Time, 1);// <<--------------------
                                }

                                Console.WriteLine("\nReceived: " + RcvCounter + ", Sent: " + SentCounter +
                                                 ", ReceivedI: " + RcvICounter + ", SentI: " + SentICounter +
                                                 ", Time elapsed: " + sw.Elapsed + "\n");

                            }
                        }

                        else if (rcvMsg.Contains("GAMMA")) //Gamma event message
                        {
                            RcvICounter++;
                            ColorLine("Gamma response---------------------------------------------" +
                                          "----------------------------------------------------------", ConsoleColor.Blue);
                            ColorLine("Received: " + rcvMsg + "\n", ConsoleColor.Blue);

                            log.Info("GAMMA " + rcvMsg); //

                            EventGamma gamma = JsonConvert.DeserializeObject<EventGamma>(rcvMsg);//
                            var nested = JsonConvert.DeserializeObject<GammaNestedJson>(gamma.Data.Parameters.Comment);


                            gamma.Data.Time = DateTime.Parse(gamma.Data.Time.ToString("yyyy-MM-dd HH:mm:ss.ff"));//round millis

                            int gammaId = 7; //7 = undefined incident

                            for (int i = 0; i < GammaDict.ListData.Count; i++)
                            {

                                if (nested.Comment.Contains(GammaDict.ListData[i].Description))
                                {
                                    Console.WriteLine("Gamma at: " + gamma.Data.Time + "." + gamma.Data.Time.Millisecond + " Gamma identified as " + GammaDict.ListData[i].IdGamma);
                                    gammaId = GammaDict.ListData[i].IdGamma;

                                    break;
                                }

                            }

                            if (gammaId == 7)
                            {
                                log.Info("UNIDENTIFIED GAMMA " + rcvMsg);
                            }

                            bool identified = false;

                            for (int i = 0; i < SourceDict.ListData.Count; i++)
                            {

                                if (gamma.Data.Parameters.__Source == SourceDict.ListData[i].SourceName)
                                {
                                    Console.WriteLine("gamma event at: " + gamma.Data.Time + "." + gamma.Data.Time.Millisecond + " source identified as " + SourceDict.ListData[i].SourceId);

                                    PushPgGamma(gammaId.ToString(), SourceDict.ListData[i].SourceId.ToString(), nested.Comment, gamma.Data.Time, nested.AlphaId, nested.Visualization, 1);

                                    identified = true;
                                    SaveImg(gamma, nested.AlphaId, nested.Visualization, SourceDict.ListData[i].SourceId.ToString(), gammaId.ToString());

                                    break;
                                }
                            }

                            if (!identified)
                            {
                                ColorLine("gamma event source at time " + gamma.Data.Time + " NOT identified", ConsoleColor.Red);
                                log.Error("failed to identify gamma event source dt: " + gamma.Data.Time);

                                log.Error(gamma.Data.Id + " " + gamma.Data.Parameters.__Source + " " + nested.Comment + " " + gamma.Data.Time + " " + nested.AlphaId + " " + nested.Visualization);

                                await PushSourceDictionaryLine(gamma.Data.Parameters.__Source, 1);
                                await GetSourceDictionary();

                                Console.WriteLine("gamma event source id must be: " + SourceDict.ListData[SourceDict.ListData.Count - 1].SourceId.ToString());// index != count

                                PushPgGamma(gammaId.ToString(), SourceDict.ListData[SourceDict.ListData.Count - 1].SourceId.ToString(), nested.Comment, gamma.Data.Time, nested.AlphaId, nested.Visualization, 1);

                                SaveImg(gamma, nested.AlphaId, nested.Visualization, SourceDict.ListData[SourceDict.ListData.Count - 1].SourceId.ToString(), gammaId.ToString());
                            }

                        }

                        else
                        {
                            ColorLine("undefined response---------------------------------------------" +
                                              "---------------------------------------------------------", ConsoleColor.Yellow);
                            ColorLine("Received: " + rcvMsg + "\n", ConsoleColor.Yellow);
                        }
                    });

                    Console.WriteLine("authenticating: " + keyJson);

                    client.Start();
                    client.Send(keyJson);

                    foreach (var item in Subscriptions)
                    {
                        client.Send(item);
                    }

                    client.Send(getTime);
                    exitEvent.WaitOne();
                }
            }

            catch (Exception ex)
            {
                ColorLine("ERROR: " + ex.ToString(), ConsoleColor.Red);
                log.Error(ex.ToString());
            }
        }
    }
}
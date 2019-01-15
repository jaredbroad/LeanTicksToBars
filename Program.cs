using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using QuantConnect;
using QuantConnect.Configuration;
using QuantConnect.Data.Market;
using QuantConnect.ToolBox;

namespace LeanTicksToBars
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                // HELLO and HI
                Console.WriteLine("Usage: LeanTicksToBars SYMBOLS TYPE MARKET FROMDATE TODATE");
                Console.WriteLine("SYMBOLS = "EURUSD");
                Console.WriteLine("TYPE = Forex/CR");
                Console.WriteLine("MARKET =   oanda");
                Console.WriteLine("FROMDATE = 2017/11/20");
                Console.WriteLine("TODATE = 2019/12/05");
                Environment.Exit(1);
            }

            // Load settings from command line
            var tickers = args[0].Split(',');
            var securityType = (SecurityType)Enum.Parse(typeof(SecurityType), args[1]);
            var market = args[2].ToLower();
            var startDate = DateTime.ParseExact(args[3], "yyyyMMdd", CultureInfo.InvariantCulture);
            var endDate = DateTime.ParseExact(args[4], "yyyyMMdd", CultureInfo.InvariantCulture);

            // Load settings from config.json
            var dataDirectory = Config.Get("data-folder", "../../../Lean/Data/");

            var rootPath = Path.Combine(dataDirectory, securityType.ToString().ToLower(), market, "tick");

            foreach (var ticker in tickers)
            {
                var symbol = Symbol.Create(ticker, securityType, market);

                var path = Path.Combine(rootPath, ticker.ToLower());
                Console.WriteLine(path);

                var date = startDate;
                while (date <= endDate)
                {
                    var fileName = Path.Combine(path, date.ToString("yyyyMMdd") + "_quote.zip");

                    if (File.Exists(fileName))
                    {
                        Console.WriteLine(fileName);

                        var ticks = new List<Tick>();

                        // Read tick data
                        using (var zip = new StreamReader(fileName))
                        {
                            using (var reader = Compression.UnzipStream(zip.BaseStream))
                            {
                                string line;
                                while ((line = reader.ReadLine()) != null)
                                {
                                    var csv = line.ToCsv(3);
                                    var time = date.Date.AddMilliseconds(csv[0].ToInt64());
                                    var bid = csv[1].ToDecimal();
                                    var ask = csv[2].ToDecimal();
                                    var tick = new Tick(time, symbol, bid, ask);
                                    ticks.Add(tick);
                                }
                            }
                        }

                        // Save the data (all resolutions)
                        foreach (var res in new[] { Resolution.Second, Resolution.Minute, Resolution.Hour, Resolution.Daily })
                        {
                            var resData = AggregateTicks(symbol, ticks, res.ToTimeSpan());

                            var writer = new LeanDataWriter(securityType, res, symbol, dataDirectory, market);
                            writer.Write(resData);
                        }
                    }

                    date = date.AddDays(1);
                }
            }
        }

        internal static IEnumerable<TradeBar> AggregateTicks(Symbol symbol, IEnumerable<Tick> ticks, TimeSpan resolution)
        {
            return
                (from t in ticks
                 group t by t.Time.RoundDown(resolution)
                     into g
                     select new TradeBar
                     {
                         Symbol = symbol,
                         Time = g.Key,
                         Open = g.First().LastPrice,
                         High = g.Max(t => t.LastPrice),
                         Low = g.Min(t => t.LastPrice),
                         Close = g.Last().LastPrice
                     });
        }

    }
}

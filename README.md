##Binance to Google Sheets v2!

This is essentially a fork from diegomanuel's Binance to Google Sheets script set.

This accompanies the Lido tutorial linking to this repository.

Attached are the two files needed for it to work: BINANCE.gs and appsscript.json.

Some quick commands using BINANCE function (directly taken from diegomanuel's repository):

### Operation: `"prices"` (public)
`=BINANCE("prices")` will return a list with the latest prices from Binance.
* `=BINANCE("prices", "BTC")` Optionally you can give a symbol to just return its price (against `USDT` by default).
* `=BINANCE("prices", "BNB", "BTC")` Optionally you can give a ticker to compare against and to just return its price.
* `=BINANCE("prices", A1:A3)` Optionally you can give a ticker range to return a list of symbols and prices.
    * Values must be simple symbols like `A1="BTC"`, `A2="ETH"` and `A3="LTC"`.
* `=BINANCE("prices", A1:A3, "headers: false")` Optionally you can give more options like not returning table headers.
* `=BINANCE("prices", A1:A3, "ticker: BNB, prices: true")` Optionally you can return only the prices (and give a ticker in the meantime).

### Operation: `"history"` (public)
`=BINANCE("history", "BTCUSDT", "interval: 1h, limit: 24")` will return a list with the latest **24hr** OHLCV **hourly** data for given **full symbol/ticker** from Binance.  
* The **2nd** parameter is **required** and it must be a **valid** single full symbol/ticker, like: `"BTCUSDT"`, `"ETHBTC"`, etc.
* The **3rd** parameter supports the following options, in a single string separated by commas:
    * `"interval: 15m"`: Can be any supported value by Binance API, like: `1m`, `5m`, `1h`, `1d`, etc. Defaults to: `1h`
    * `"start: 2021-01-01"`: Optional. The start date/time to get data from. Can be any supported value by `new Date(value)` JS object, like: `2021-01-01-11-22-33` (time is converted to `:` format).
    * `"end: 2021-01-31"`: Optional. The end date/time to get data from. The same supported values as `start` option.
    * `"limit: 10"`: Optional. How many rows to return, from `1` to `1000`, defaults to: `500`.
    * `"headers: false"`: Don't return table headers.
* **IMPORTANT:** If any parameter value is wrong, Binance API will answer with a `400` status code.

### Operation: `"stats/24h"` (public)
`=BINANCE("stats/24h", A1:A3)` will return a list with the 24hs stats for given symbols from Binance.
* A single value like `"BTC"` or a range of values is **required**. Values must be simple symbols like `A1="BTC"`, `A2="ETH"` and `A3="LTC"`.
* `=BINANCE("stats/24h", A1:A3, "BTC")` Optionally you can give a ticker to match against (defaults to `USDT`).
* `=BINANCE("stats/24h", A1:A3, "ticker: BTC, headers: false")` Optionally you can give more options like not returning table headers.

To learn more about this solution, check diegomanuel's guide:

https://github.com/diegomanuel/binance-to-google-sheets

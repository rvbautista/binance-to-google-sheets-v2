/**
 * Binance to Google Sheets!
 * Diego Manuel - diegomanuel@gmail.com
 * https://github.com/diegomanuel/binance-to-google-sheets
 * https://github.com/diegomanuel/binance-to-google-sheets-proxy
 */


/**
 * Main formula wrapper to call supported operations.
 *
 * @param operation The operation tag to call.
 * @param range_or_cell A range of cells or a single cell or a literal/string value.
 * @param opts Additional options like the symbol/ticker to match against or an option list like "ticker: USDT, headers: false" (depending the called operation).
 * @param force_refresh_cell Cells are automatically refreshed, but you can force it by passing any changing value here.
 * @return Depends on the operation given.
 * @customfunction
 */
function BINANCE(operation, range_or_cell, opts, force_refresh_cell) {
  const options = BinUtils().parseOptions(opts);

  if (DEBUG) {
    Logger.log("OP: "+operation);
    Logger.log("RANGE: "+JSON.stringify(range_or_cell));
    Logger.log("OPTS: "+JSON.stringify(opts));
    Logger.log("OPTIONS: "+JSON.stringify(options));
  }

  if (operation == "version") {
    return VERSION;
  }
  if (BinDoLastUpdate().is(operation)) {
    return BinDoLastUpdate().run();
  }
  if (BinDoCurrentPrices().is(operation)) {
    return BinDoCurrentPrices().run(range_or_cell, options);
  }
  if (BinDoHistoricalData().is(operation)) {
    return BinDoHistoricalData().run(range_or_cell, options);
  }
  if (BinDo24hStats().is(operation)) {
    return BinDo24hStats().run(range_or_cell, options);
  }
  if (BinDoAccountInfo().is(operation)) {
    return BinDoAccountInfo().run(range_or_cell, options);
  }
  if (BinDoOrdersOpen().is(operation)) {
    return BinDoOrdersOpen().run(range_or_cell, options);
  }
  if (BinDoOrdersDone().is(operation)) {
    return BinDoOrdersDone().run(range_or_cell, options);
  }
  if (BinDoOrdersTable().is(operation)) {
    return BinDoOrdersTable().run(range_or_cell, options);
  }
  
  throw new Error("Unsupported operation given: '"+operation+"'");
}

/**
 * This is just a dummy function, use `BINANCE` instead!
 * It only serves to help to [R]efresh (the "R" from there) spreadsheet formulas
 * by switching between `BINANCE` and `BINANCER`.
 * 
 * @customfunction
 */
function BINANCER(operation, range_or_cell, opts, force_refresh_cell) {
  return BINANCE(operation, range_or_cell, opts, force_refresh_cell);
}


/////////////////////////////////////

/**
 * Runs the table orders script.
 */
function BinDoOrdersTable() {
  const ASSETS_PROP_NAME = "BIN_ASSETS_ORDERS_TABLE";
  const header_size = 3; // How many rows the header will have
  const max_items = 1000; // How many items to be fetched on each run
  const delay = 500; // Delay between API calls in milliseconds
  let update_assets = {}; // Ugly global index of assets to be updated for the next poll run

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "orders/table";
  }

  /**
   * Returns true if the given operation belongs to this code
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    // @TODO Verify if it's still running and cancel if it is!
    return BinScheduler().getSchedule(tag()) || "10m";
  }
  
  /**
   * Establishes the current sheet as the orders table for given symbols.
   * 
   * IMPORTANT: Data written into this sheet/table should never be altered by hand!
   * You may ONLY REMOVE records from the bottom of the sheet (as many as you want, even all of them).
   *
   * @param {["BTC","ETH"..]} range_or_cell REQUIRED! Will fetch ALL historic orders for given symbols only.
   *            It can be a range with single assets like ["BTC","ETH"..] or just a single assets like "BTC".
   *            It can also be a range with full tickers like ["BTCBUSD","ETHUSDT","ETHBTC"..] if you set "ticker: range" as option.
   * @param {ticker, unchanged} options Custom options for this operation
   *            ticker: Ticker to match against (USDT by default). Example: "ticker: BUSD".
   *                    It can also be "ticker: range" to use full pairs from `range_or_cell`.
   *            unchanged: Skip assets balance check to always force to fetch for new orders. Example: "unchanged: false" will disable the check.
    *                      WARNING: The "unchanged check" is an important optimization, so disable it with caution!
   * @return The list done orders for all given assets/symbols/tickers.
   */
  function run(range_or_cell, options) {
    Logger.log("[BinDoOrdersTable] Running..");
    const range = BinUtils().getRangeOrCell(range_or_cell);
    if (!range.length) {
      throw new Error("A range with crypto symbols must be given!");
    }

    const sheets = _findSheets();
    if (sheets.length === 0) { // Ensure the formula is correctly placed at "A1"
      throw new Error("This formula must be placed at 'A1'!");
    }
    const names = _sheetNames(sheets);
    Logger.log("[BinDoOrdersTable] Currently active at '"+names.length+"' sheets: "+JSON.stringify(names));
    Logger.log("[BinDoOrdersTable] Done!");

    return [
      ["Do **NOT** add/remove/alter this table data by hand! --- Polling "+max_items+" items every "+period()+" --- Patience, you may hide this row"]
    ];
  }

  /**
   * Initializes all found sheets that were set as orders table.
   * It will re-generate the table header and remove any extra blank rows from the bottom.
   */
  function init() {
    return _findSheets().map(function(sheet) { // Go through each sheet found
      try {
        _initSheet(sheet); // Ensure the sheet is initialized
      } catch (err) {
        _setStatus(sheet, "ERROR: "+err.message);
        console.error(err);
      }
      return sheet;
    });
  }

  /**
   * Executes a poll session to download and save historic order records for each currently active sheets
   */
  function execute() {
    Logger.log("[BinDoOrdersTable] Running..");

    const sheets = _findSheets();
    const names = _sheetNames(sheets);
    Logger.log("[BinDoOrdersTable] Processing '"+names.length+"' sheets: "+JSON.stringify(names));

    if (sheets.length) { // Refresh wallet assets only if we have sheets to update!
      BinWallet().refreshAssets(true); // Exclude sub-account assets!
    }

    sheets.map(function(sheet) { // Go through each sheet found
      try {
        _initSheet(sheet); // Ensure the sheet is initialized
        _fetchAndSave(sheet); // Fetch and save data for this sheet assets
        _updateLastAssets(); // Update the latest asset balances for next run
      } catch (err) {
        _setStatus(sheet, "ERROR: "+err.message);
        console.error(err);
      }
    });

    Logger.log("[BinDoOrdersTable] Done!");
  }

  function _fetchAndSave(sheet) {
    Logger.log("[BinDoOrdersTable] Processing sheet: "+sheet.getName());
    const [range_or_cell, options] = _parseFormula(sheet);
    const ticker_against = options["ticker"] || TICKER_AGAINST;
    const range = BinUtils().getRangeOrCell(range_or_cell, sheet);
    if (!range.length) {
      throw new Error("A range with crypto symbols must be given!");
    }
    Logger.log("[BinDoOrdersTable] Processing "+range.length+" sheet's assets: "+range);

    _setStatus(sheet, "fetching data..");
    const opts = {
      "no_cache_ok": true,
      "discard_40x": true, // Discard 40x errors for disabled wallets!
      "retries": Math.max(10, Math.min(100, range.length*5)),
      "do_unchanged_check": BinUtils().parseBool(options["unchanged"], undefined)
    };

    // Fetch data for given symbols in range
    const data = range.reduce(function(rows, asset) {
      const numrows = rows.length;
      const symbol = _fullSymbol(asset, ticker_against);
      if (numrows > max_items) { // Skip data fetch if we hit max items cap!
        Logger.log("[BinDoOrdersTable] Max items cap! ["+numrows+"/"+max_items+"] => Skipping fetch for: "+symbol);
        return rows;
      }

      const symbol_data = _fetch(numrows, sheet, asset, ticker_against, opts);
      Logger.log("[BinDoOrdersTable] Fetched "+symbol_data.length+" records for: "+symbol);
      return rows.concat(symbol_data);
    }, []);
  
    // Parse and save collected data
    const parsed = _parseData(data);
    if (parsed.length) {
      Logger.log("[BinDoOrdersTable] Saving "+parsed.length+" downloaded records into '"+sheet.getName()+"' sheet..");
      _setStatus(sheet, "saving "+parsed.length+" records..");
      _insertData(sheet, parsed);
    } else {
      Logger.log("[BinDoOrdersTable] No records downloaded for sheet: "+sheet.getName());
    }

    // Update some stats on sheet
    _setStatus(sheet, "done / waiting");
    _updateStats(sheet, parsed);
  }

  function _fullSymbol(asset, ticker) {
    return ticker.toLowerCase() === "range" ? asset+"" : asset+""+ticker
  }

  /**
   * Returns true if the given asset was changed its "net" property from last run
   * If it's unchanged and returns false, it will skip fetching orders for it!
   */
  function _isUnchangedAsset(type, asset) {
    if (type === "futures") {
      return false; // @TODO Improve this, since futures asset balances don't change the quote asset!
    }
    const last = _getLastAssets(type, asset);
    const current = BinWallet().getAssets(type, asset);
    return (last ? last.net : undefined) === (current ? current.net : undefined);
  }

  function _fetch(numrows, sheet, asset, ticker, opts) {
    const data_spot = _fetchOrders("spot", numrows, sheet, asset, ticker, opts); // Get SPOT orders
    numrows += data_spot.length;
    const data_cross = _fetchOrders("cross", numrows, sheet, asset, ticker, opts); // Get CROSS MARGIN orders
    numrows += data_cross.length;
    const data_isolated = _fetchOrders("isolated", numrows, sheet, asset, ticker, opts); // Get ISOLATED MARGIN orders
    numrows += data_isolated.length;
    const data_futures = _fetchOrders("futures", numrows, sheet, asset, ticker, opts); // Get FUTURES orders
    return [...data_spot, ...data_cross, ...data_isolated, ...data_futures];
  }

  function _fetchOrders(type, numrows, sheet, asset, ticker, opts) {
    if (!BinWallet().isEnabled(type)) {
      Logger.log("[BinDoOrdersTable]["+type.toUpperCase()+"] Skipping disabled wallet.");
      return [];
    }
    if (opts["do_unchanged_check"] && _isUnchangedAsset(type, asset)) { // Skip data fetch if the asset balance hasn't changed from last run!
      Logger.log("[BinDoOrdersTable]["+type.toUpperCase()+"] Skipping unchanged asset: "+asset);
      return [];
    }

    const symbol = _fullSymbol(asset, ticker);
    const [fkey, fval] = _parseFilterQS(sheet, symbol, type);
    const limit = Math.max(2, Math.min(max_items, max_items - numrows + (fkey === "fromId" ? 1 : 0))); // Add 1 more result since it's going to be skipped
    const qs = "limit="+limit+"&symbol="+symbol+(fkey&&fval?"&"+fkey+"="+fval:"");
    let data = [];

    if (numrows >= max_items) { // Skip data fetch if we hit max items cap!
      Logger.log("[BinDoOrdersTable]["+type.toUpperCase()+"] Max items cap! ["+numrows+"/"+max_items+"] => Skipping fetch for: "+symbol);
      return [];
    }

    try {
      Utilities.sleep(delay); // Add some waiting time to avoid 418 responses!
      Logger.log("[BinDoOrdersTable]["+type.toUpperCase()+"] Fetching orders for '"+symbol+"'..");
      if (type === "spot") { // Get SPOT orders
        data = _fetchSpotOrders(opts, qs);
      } else if (type === "cross") { // Get CROSS MARGIN orders
        data = _fetchCrossOrders(opts, qs);
      } else if (type === "isolated") { // Get ISOLATED MARGIN orders
        if (BinWallet().getIsolatedPairs(symbol)) {
          data = _fetchIsolatedOrders(opts, qs); // Only fetch if the symbol has a pair created in isolated account!
        } else {
          Logger.log("[BinDoOrdersTable][ISOLATED] Skipping inexistent isolated pair for: "+symbol);
          return [];
        }
      } else if (type === "futures") { // Get FUTURES orders
        data = _fetchFuturesOrders(opts, qs);
      } else {
        throw new Error("Bad developer.. shame on you!  =0");
      }
      if (data.length !== limit) { // We got all possible rows so far on this run, we don't have more to fetch, so..
        _markUpdateAsset(type, asset); // Mark the asset to update its balance for next run!
      }
      if (fkey === "fromId") { // Skip the first result if we used fromId to filter
        data.shift();
      }
      Logger.log("[BinDoOrdersTable]["+type.toUpperCase()+"] Fetched '"+data.length+"' orders for '"+symbol+"'..");
      return data;
    } catch (err) { // Discard request errors and keep running!
      console.error("[BinDoOrdersTable]["+type.toUpperCase()+"] Couldn't fetch orders for '"+symbol+"': "+err.message);
      return [];
    }
  }

  function _fetchSpotOrders(opts, qs) {
    return _fetchOrdersForType("spot", opts, "api/v3/myTrades", qs);
  }

  function _fetchCrossOrders(opts, qs) {
    return _fetchOrdersForType("cross", opts, "sapi/v1/margin/myTrades", qs);
  }

  function _fetchIsolatedOrders(opts, qs) {
    return _fetchOrdersForType("isolated", opts, "sapi/v1/margin/myTrades", "isIsolated=true&"+qs);
  }

  function _fetchFuturesOrders(opts, qs) {
    const options = Object.assign({futures: true}, opts);
    return _fetchOrdersForType("futures", options, "fapi/v1/userTrades", qs)
      .map(function(order) {
        return Object.assign({
          isMaker: order.maker,
          isBuyer: order.buyer,
        }, order);
      });
  }

  function _fetchOrdersForType(type, opts, url, qs) {
    const orders = new BinRequest(opts).get(url, qs);
    return (orders||[]).map(function(order) {
      order.market = type.toUpperCase(); // NOTE: Very important to be added for future last row #ID matching!
      return order;
    });
  }

  function _findSheets() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const self = BinDoOrdersTable();

    return ss.getSheets().filter(function(sheet) {
      const formula = _getFormula(sheet);
      return formula && BinUtils().isFormulaMatching(self, self.period(), formula);
    });
  }

  function _sheetNames(sheets) {
    return sheets.map(function(sheet) {
      return sheet.getName();
    });
  }

  function _initSheet(sheet) {
    sheet.setFrozenRows(header_size); // Freeze header rows
    sheet.getRange("A1:J1").mergeAcross();
    sheet.getRange("B2:C2").mergeAcross();
    sheet.getRange("E2:F2").mergeAcross();

    // Set the table headers
    const header = ["Trade #ID", "Order #ID", "Date", "Pair", "Type", "Side", "Price", "Amount", "Commission", "Total"];
    sheet.getRange("A3:J3").setValues([header]);
    sheet.getRange("A2").setValue("Last poll:");
    sheet.getRange("D2").setValue("Status:");
    sheet.getRange("G2").setValue("Records:");
    sheet.getRange("I2").setValue("Pairs:");

    // Set initial stats values
    _initCellValue(sheet, "B2");
    _initCellValue(sheet, "E2", "waiting for 1st poll run");
    _initCellValue(sheet, "H2", 0);
    _initCellValue(sheet, "J2", 0);

    // Remove extra rows (if any)
    const row_max = Math.max(header_size+1, sheet.getLastRow());
    const row_diff = sheet.getMaxRows() - row_max;
    if (row_diff > 0) {
      sheet.deleteRows(row_max+1, row_diff);
    }
    // Remove extra colums (if any)
    const col_diff = sheet.getMaxColumns() - header.length;
    if (col_diff > 0) {
      sheet.deleteColumns(header.length+1, col_diff);
    }

    // Set styles & formats
    const bold = SpreadsheetApp.newTextStyle().setBold(true).build();
    const italic = SpreadsheetApp.newTextStyle().setItalic(true).build();
    sheet.getRange("A1:J"+header_size) // Styles for the whole header
      .setHorizontalAlignment("center")
      .setVerticalAlignment("middle")
      .setTextStyle(bold);
    sheet.getRange("E2").setTextStyle(italic);
    sheet.getRange("B2").setNumberFormat("ddd d hh:mm");
  }

  function _parseFilterQS(sheet, symbol, type) {
    const row = _findLastRowData(sheet, symbol, type);
    if (row) { // We found the latest matching row for this symbol and type..
      return ["fromId", row[0]]; // .. so use its #ID value!
    }

    if (type === "futures") { // @TODO REVIEW: The 'startTime' filter is not working fine on futures API....!!
      return [];
    }

    // Fallback to the oldest possible datetime (Binance launch date)
    const start_time = new Date("2017-01-01T00:00:00.000Z").getTime();
    return ["startTime", Math.floor(start_time / 1000)];
  }

  function _findLastRowData(sheet, symbol, type) {
    const last_row = sheet.getLastRow();
    const last_col = sheet.getLastColumn();

    for (let row_idx = last_row; row_idx >= header_size+1 ; row_idx--) {
      const range = sheet.getRange(row_idx, 1, 1, last_col);
      const [row] = range.getValues();
      if (_isRowMatching(row, symbol, type)) { // We found the latest matching row!
        if (DEBUG) {
          Logger.log("Found last row data at idx ["+row_idx+"] for '"+symbol+"@"+type+"' with: "+JSON.stringify(row));
        }
        return row;
      }
    }

    return null;
  }

  /**
   * WARNING: This function is CRUCIAL to find the right #ID to be used to fetch orders starting from it!
   * If it returns the wrong answer, a wrong ID will be used and it could cause to fetch and save
   * duplicated orders (ones that were already saved in previous poll runs) because it doesn't check
   * for duplicates when inserting rows into the sheet.. and mainly, because it could be a process-expensive check to do
   * if we already have LOTS of rows in the sheet (@TODO: but maybe I should do the duplicates check anyways, just in case..?)
   */
  function _isRowMatching(row, symbol, type) {
    return row[3] === symbol // Matches the symbol?
      // Matches the market type?
      && (row[4].match(/\s\-\s/)
        // The row has a type with " - " in it => It should exactly match the given type
        ? new RegExp(type+"\\s\\-\\s", "i").test(row[4])
        // The row has NOT a type with " - " in it => Backward compatibility: consider as SPOT
        : (type === "spot")); // @TODO This could be removed in a few future releases
  }

  function _getFormula(sheet) {
    try { // [#13] It may fail when a chart is moved to its own sheet!
      return sheet.getRange("A1").getFormula();
    } catch(_) {
      return "";
    }
  }

  function _parseFormula(sheet) {
    const formula = _getFormula(sheet);
    const self = BinDoOrdersTable();
    const [range_or_cell, options] = BinUtils().extractFormulaParams(self, formula);
    if (DEBUG) {
      Logger.log("Parsed formula range: "+JSON.stringify(range_or_cell));
      Logger.log("Parsed formula options: "+JSON.stringify(options));
    }
    // Just to be clear that this is the expected return
    return [range_or_cell, options];
  }

  function _parseData(data) {
    return data.reduce(function(rows, order) {
      const price = BinUtils().parsePrice(order.price);
      const amount = parseFloat(order.qty);
      const commission = BinUtils().parsePrice(order.commission);
      const row = [
        order.id, // NOTE: Used for future last row #ID matching!
        order.orderId,
        new Date(parseInt(order.time)),
        order.symbol, // NOTE: Used for future last row #ID matching!
        order.market + " - " + (order.isMaker ? "LIMIT" : "STOP-LIMIT"), // NOTE: Used for future last row #ID matching!
        order.isBuyer ? "BUY" : "SELL",
        price,
        amount,
        commission,
        price*amount
      ];
      rows.push(row);
      return rows;
    }, []);
  }

  function _insertData(sheet, data) {
    const last_row = Math.max(sheet.getLastRow(), header_size);
    const last_col = sheet.getLastColumn();
    const dlen = data.length;

    sheet.insertRowsAfter(last_row, dlen);
    const range = sheet.getRange(last_row+1, 1, dlen, last_col);
    range.setValues(data);

    // Sort ALL sheet's rows!
    _getSheetDataRange(sheet).sort(3);
  }

  function _setStatus(sheet, status) {
    sheet.getRange("E2").setValue(status);
  }

  function _updateStats(sheet, saved_data) {
    // Calculate total orders per pair
    const row_max = Math.max(header_size+1, sheet.getLastRow());
    const pairs = sheet.getRange("D"+(header_size+1)+":D"+row_max).getValues();
    const [count, totals] = pairs.reduce(function([count, acc], [pair]) {
      if (pair) {
        acc[pair] = 1 + (acc[pair]||0);
        count += 1;
      }
      return [count, acc];
    }, [0, {}]);

    sheet.getRange("H2").setValue(count);
    sheet.getRange("J2").setValue(Object.keys(totals).length);
    Logger.log("[BinDoOrdersTable] Sheet '"+sheet.getName()+"' total orders per pair:\n"+JSON.stringify(totals));

    sheet.getRange("B2").setValue(new Date()); // Update last run time
  }

  function _initCellValue(sheet, cell, emptyval) {
    if (!sheet.getRange(cell).getValue()) {
      sheet.getRange(cell).setValue(emptyval !== undefined ? emptyval : "-");
    }
  }

  function _getLastAssets(type, asset) {
    const data = PropertiesService.getScriptProperties().getProperty(ASSETS_PROP_NAME+"_"+type.toUpperCase());
    const assets = data ? JSON.parse(data) : {};
    return asset ? assets[asset] : assets;
  }

  function _markUpdateAsset(type, asset) {
    update_assets[type] = update_assets[type] || {};
    update_assets[type][asset] = BinWallet().getAssets(type, asset);
  }

  function _updateLastAssets() {
    return Object.keys(update_assets).map(function(type) {
      const assets = _getLastAssets(type) || {};
      Object.keys(update_assets[type]).map(function(asset) {
        assets[asset] = update_assets[type][asset];
      });

      return PropertiesService.getScriptProperties().setProperty(ASSETS_PROP_NAME+"_"+type.toUpperCase(), JSON.stringify(assets));
    });
  }

  /**
   * Get the FULL data range for given sheet
   */
  function _getSheetDataRange(sheet) {
    const row_max = Math.max(header_size+1, sheet.getLastRow());
    return sheet.getRange("A"+(header_size+1)+":J"+row_max);
  }

  /**
   * Returns ALL the rows contained in ALL defined sheets as order tables
   */
  function getRows() {
    return _findSheets().reduce(function(rows, sheet) { // Go through each sheet found
      const values = _getSheetDataRange(sheet).getValues().filter(function(val) {
        return val && val[0]; // Filter possible empty rows!
      });
      return values && values.length ? rows.concat(values) : rows;
    }, []);
  }

  /**
   * Returns true if at least ONE sheet in the spreadsheet is defined as orders table
   */
  function hasSheets() {
    return _findSheets().length > 0;
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    run,
    init,
    execute,
    getRows,
    hasSheets
  };
}/**
 * Runs the historical data script.
 * 
 * Thanks to @almeidaaraujo (https://github.com/almeidaaraujo) for his first approach!
 * PR: https://github.com/diegomanuel/binance-to-google-sheets/pull/22
 * Issue: https://github.com/diegomanuel/binance-to-google-sheets/issues/21
 */
function BinDoHistoricalData() {
  let lock_retries = 5; // Max retries to acquire lock

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "history";
  }

  /**
   * Returns true if the given operation belongs to this code
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    return BinScheduler().getSchedule(tag()) || "10m";
  }

  /**
   * Returns historical market OHLCV data for a single given symbol/ticker.
   *
   * @param symbol REQUIRED! A full symbol/ticker to get its data.
   * @param options An option list string like "interval: $interval, start: $start, end: $end, limit: $l, headers: false"
   *                      $interval: 1m, 5m, 1h, 1d, 1w, ... (defaults to: 1h)
   *                      $start: 2021-01-01 00:00 (with or without time given, defaults to: none/no-time)
   *                      $end: 2021-03-01 (with or without time given, defaults to: none/no-time)
   *                      $l: from 1 to 1000 (max items set by Binance, defaults to: 500)
   * @return The list of OHLCV data.
   */
  function run(symbol, options) {
    Logger.log("[BinDoHistoricalData] Running..");
    if (!symbol) {
      throw new Error("A full symbol/ticker like 'BTCUSDT' must be given!");
    }

    const bs = BinScheduler();
    try {
      bs.clearFailed(tag());
      const data = execute(symbol, options);
      Logger.log("[BinDoHistoricalData] Done!");
      return data;
    } catch(err) { // Re-schedule this failed run!
      bs.rescheduleFailed(tag());
      throw err;
    }
  }

  function execute(symbol, options) {
    const lock = BinUtils().getUserLock(lock_retries--);
    if (!lock) { // Could not acquire lock! => Retry
      return execute(symbol, options);
    }

    const {interval, start, end, limit} = options;
    const qs = ["symbol="+symbol];
    qs.push("interval="+(interval||"1h"));
    if (start) {
      qs.push("startTime="+(BinUtils().parseDate(start).getTime()));
    }
    if (end) {
      qs.push("endTime="+(BinUtils().parseDate(end).getTime()));
    }
    if (limit) {
      qs.push("limit="+Math.max(1, Math.min(1000, parseInt(limit))));
    }

    const opts = {"no_cache_ok": true, retries: 20, public: true};
    const data = new BinRequest(opts).get("api/v3/klines", qs.join("&"));
    BinUtils().releaseLock(lock);
    return parse(data, options);
  }

  function parse(data, options) {
    // Each row has the following data:
    // [
    //   1499040000000,      // Open time
    //   "0.01634790",       // Open
    //   "0.80000000",       // High
    //   "0.01575800",       // Low
    //   "0.01577100",       // Close
    //   "148976.11427815",  // Volume
    //   1499644799999,      // Close time
    //   "2434.19055334",    // Quote asset volume
    //   308,                // Number of trades
    //   "1756.87402397",    // Taker buy base asset volume
    //   "28.46694368",      // Taker buy quote asset volume
    //   "17928899.62484339" // Ignore.
    // ]
    const show_headers = BinUtils().parseBool(options.headers);
    const header = ["Open Time", "Open", "High", "Low", "Close", "Close Time", "Volume", "Trades"];
    const parsed = data.reduce(function(rows, d) {
      const row = [
        new Date(parseInt(d[0])),
        parseFloat(d[1]),
        parseFloat(d[2]),
        parseFloat(d[3]),
        parseFloat(d[4]),
        new Date(parseInt(d[6])),
        parseFloat(d[5]),
        parseInt(d[7])
      ];
      return rows.concat([row]);
    }, []);

    Logger.log("[BinDoHistoricalData] Returning "+parsed.length+" rows..");
    return show_headers ? [header, ...parsed] : parsed;
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    run
  };
}/**
 * Runs the account info script.
 */
function BinDoAccountInfo() {
  let lock_retries = 5; // Max retries to acquire lock

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "account";
  }

  /**
   * Returns true if the given operation belongs to this code
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    return BinScheduler().getSchedule(tag()) || "10m";
  }

  /**
   * Schedules this operation to be run in the next "1m" trigger
   */
  function schedule() {
    return BinScheduler().rescheduleFailed(tag());
  }

  /**
   * Fetches fresh data for each implemented and enabled Binance wallet
   * that will be parsed and saved inside each `run/2` call.
   */
  function refresh(exclude_sub_accounts) {
    const opts = {headers: false};
    const bw = BinWallet();

    run("spot", opts);
    if (bw.isEnabled("cross")) {
      run("cross", opts);
    }
    if (bw.isEnabled("isolated")) {
      run("isolated", opts);
    }
    if (bw.isEnabled("futures")) {
      run("futures", opts);
    }
    if (!exclude_sub_accounts) { // Include sub-account assets
      run("sub", opts);
    }
  }

  /**
   * Gets the list of ALL sub-accounts
   */
  function listSubAccounts() {
    const data = new BinRequest().get("sapi/v1/sub-account/list");
    return data && data.subAccounts ? data.subAccounts : [];
  }

  /**
   * Returns account information for given type of wallet (or general/overview if none given).
   *
   * @param type The account wallet type to display info: -none-, "spot", "cross", "isolated"
   * @param options An option list like "headers: false"
   * @return A table with account information
   */
  function run(type, options) {
    try {
      BinScheduler().clearFailed(tag());
      return execute((type||"").toLowerCase()||"overview", options);
    } catch(err) { // Re-schedule this failed run!
      schedule();
      throw err;
    }
  }

  function execute(type, options) {
    Logger.log("[BinDoAccountInfo]["+type.toUpperCase()+"] Running..");
    const wallet_type = type === "futures/positions" ? "futures" : type;
    if (!BinWallet().isEnabled(wallet_type)) { // The "overview" case will always be true
      Logger.log("[BinDoAccountInfo]["+type.toUpperCase()+"] The wallet is disabled!");
      return [["The "+type.toUpperCase()+" wallet is disabled! Enable it from 'Binance->Wallets' main menu."]];
    }

    const lock = BinUtils().getUserLock(lock_retries--);
    if (!lock) { // Could not acquire lock! => Retry
      return execute(type, options);
    }
    
    const opts = {CACHE_TTL: 55};
    const data = request(type, opts);
  
    BinUtils().releaseLock(lock);
    const parsed = parse(type, data, options);
    Logger.log("[BinDoAccountInfo]["+type.toUpperCase()+"] Done!");
    return parsed;
  }

  // @TODO Add filter for empty assets (balance=0) at BinRequest().get() options
  function request(type, opts) {
    if (type === "overview") { // Ensure to fetch fresh data from all wallets for the overview
      refresh();
      return; // We don't return any data here!
    }
    const wallet_type = type === "futures/positions" ? "futures" : type;
    if (!BinWallet().isEnabled(wallet_type)) { // The wallet is disabled..
      return; // ..so we don't return any data here!
    }

    const br = new BinRequest(opts);
    if (type === "spot") {
      return br.get("api/v3/account", "", "");
    }
    if (type === "cross") {
      return br.get("sapi/v1/margin/account", "", "");
    }
    if (type === "isolated") {
      return br.get("sapi/v1/margin/isolated/account", "", "");
    }
    if (type === "futures" || type === "futures/positions") {
      const options = Object.assign({futures: true}, opts);
      return new BinRequest(options).get("fapi/v2/account", "", "");
    }
    if (type === "sub") {
      return _requestSubAccounts(opts);
    }

    throw new Error("Unsupported account wallet type: "+type);
  }

  function _requestSubAccounts(opts) {
    const subaccs = BinSetup().getSubAccounts();
    
    return Object.keys(subaccs).reduce(function(assets, email) {
      const qs = "email="+email;
      const data = new BinRequest(opts).get("sapi/v3/sub-account/assets", qs);
      assets[email] = (data||{}).balances || [];
      return assets;
    }, {});
  }

  function parse(type, data, {headers: show_headers}) {
    show_headers = BinUtils().parseBool(show_headers);
    if (type === "overview") {
      return parseOverview(show_headers);
    }
    const wallet_type = type === "futures/positions" ? "futures" : type;
    if (!BinWallet().isEnabled(wallet_type)) { // The wallet is disabled..
      return []; // ..so we return empty data here!
    }

    if (type === "spot") {
      return parseSpot(data, show_headers);
    }
    if (type === "cross") {
      return parseCrossMargin(data, show_headers);
    }
    if (type === "isolated") {
      return parseIsolatedMargin(data, show_headers);
    }
    if (type === "futures") {
      return parseFutures(data, show_headers);
    }
    if (type === "futures/positions") {
      return parseFuturesPositions(data, show_headers);
    }
    if (type === "sub") {
      return parseSubAccounts(data, show_headers);
    }

    throw new Error("Unsupported account wallet type: "+type);
  }

  function parseOverview(show_headers) {
    const headers = ["Asset", "Free", "Locked", "Borrowed", "Interest", "Total", "Net"];
    const assets = BinWallet().calculateAssets(); // Calculate assets from ALL implemented/available wallets!
    const balances = Object.keys(assets).map(function(symbol) {
      const asset = assets[symbol];
      return [
        symbol,
        asset.free,
        asset.locked,
        asset.borrowed,
        asset.interest,
        asset.total,
        asset.net
      ];
    }, []);

    const sorted = BinUtils().sortResults(balances);
    return show_headers ? [headers, ...sorted] : sorted;
  }

  function parseSpot(data, show_headers) {
    const wallet = BinWallet();
    const header1 = ["Account Type", "Maker Commission", "Taker Commission", "Buyer Commission", "Seller Commission", "Can Trade", "Can Withdraw", "Can Deposit", "Last Update"];
    const header2 = ["Asset", "Free", "Locked", "Total"];
    const account = ["Spot", data.makerCommission, data.takerCommission, data.buyerCommission, data.sellerCommission, data.canTrade, data.canWithdraw, data.canDeposit, new Date()];
    const general = show_headers ? [header1, account, header2] : [account];

    const assets = [];
    const balances = (data.balances || []).reduce(function(rows, a) {
      const asset = wallet.parseSpotAsset(a);
      if (asset.total > 0) { // Only return assets with balance
        assets.push(asset);
        rows.push([
          asset.symbol,
          asset.free,
          asset.locked,
          asset.total
        ]);
      }
      return rows;
    }, []);

    // Save assets to wallet
    wallet.setSpotAssets(assets);

    const sorted = BinUtils().sortResults(balances);
    return [...general, ...sorted];
  }

  function parseCrossMargin(data, show_headers) {
    const wallet = BinWallet();
    const header1 = ["Account Type", "Trade Enabled", "Transfer Enabled", "Borrow Enabled", "Margin Level", "Total BTC Asset", "Total BTC Liability", "Total BTC Net Asset", "Last Update"];
    const header2 = ["Asset", "Free", "Locked", "Borrowed", "Interest", "Total", "Net"];
    const account = ["Cross Margin", data.tradeEnabled, data.transferEnabled, data.borrowEnabled, data.marginLevel, data.totalAssetOfBtc, data.totalLiabilityOfBtc, data.totalNetAssetOfBtc, new Date()];
    const general = show_headers ? [header1, account, header2] : [account];

    const assets = [];
    const balances = (data.userAssets || []).reduce(function(rows, a) {
      const asset = wallet.parseCrossMarginAsset(a);
      if (asset.total !== 0 || asset.net !== 0) { // Only return assets with balance
        assets.push(asset);
        rows.push([
          asset.symbol,
          asset.free,
          asset.locked,
          asset.borrowed,
          asset.interest,
          asset.total,
          asset.net
        ]);
      }
      return rows;
    }, []);

    // Save assets to wallet
    wallet.setCrossAssets(assets);

    const sorted = BinUtils().sortResults(balances);
    return [...general, ...sorted];
  }

  function parseIsolatedMargin(data, show_headers) {
    const wallet = BinWallet();
    const header1 = ["Account Type", "Total BTC Asset", "Total BTC Liability", "Total BTC Net Asset", "Last Update"];
    const account = ["Isolated Margin", data.totalAssetOfBtc, data.totalLiabilityOfBtc, data.totalNetAssetOfBtc, new Date()];
    const general = show_headers ? [header1, account] : [account];

    const pairs = [];
    const assets = [];
    const balances = (data.assets || []).reduce(function(rows, a) {
      pairs.push(a); // Add isolated pair to wallet
      if (show_headers) {
        rows.push(["Pair", "Margin Level", "Margin Ratio", "Index Price", "Liquidate Price", "Liquidate Rate"]);
      }
      const marginLevel = parseFloat(a.marginLevel);
      const marginRatio = parseFloat(a.marginRatio);
      const indexPrice = parseFloat(a.indexPrice);
      const liquidatePrice = parseFloat(a.liquidatePrice);
      const liquidateRate = parseFloat(a.liquidateRate);
      rows.push([a.symbol, marginLevel, marginRatio, indexPrice, liquidatePrice, liquidateRate]);
      if (show_headers) {
        rows.push(["Asset", "Free", "Locked", "Borrowed", "Interest", "Total", "Net", "Net BTC"]);
      }
      const baseAsset = wallet.parseIsolatedMarginAsset(a.baseAsset);
      const quoteAsset = wallet.parseIsolatedMarginAsset(a.quoteAsset);
      assets.push(baseAsset); // Add base asset to wallet
      assets.push(quoteAsset); // Add quote asset to wallet
      rows.push(parseIsolatedMarginAssetRow(baseAsset));
      rows.push(parseIsolatedMarginAssetRow(quoteAsset));
      return rows;
    }, []);

    // Save isolated pairs and assets to wallet
    wallet.setIsolatedPairs(pairs);
    wallet.setIsolatedAssets(assets);

    return [...general, ...balances];
  }

  function parseIsolatedMarginAssetRow(asset) {
    return [
      asset.symbol,
      asset.free,
      asset.locked,
      asset.borrowed,
      asset.interest,
      asset.total,
      asset.net,
      asset.netBTC
    ];
  }

  function parseFutures(data, show_headers) {
    const wallet = BinWallet();
    const header1 = ["Account Type", "Total Free", "Total Locked", "Total Wallet Balance", "Total Margin Balance", "Total Cross Balance", "Max Withdraw", "Total UnPnl", "Initial Margin", "Maint. Margin", "Last Update"];
    const totalWalletBalance = parseFloat(data.totalWalletBalance);
    const totalAvailableBalance = parseFloat(data.availableBalance);
    const account = ["Futures",
                      totalAvailableBalance,
                      totalWalletBalance - totalAvailableBalance,
                      totalWalletBalance,
                      parseFloat(data.totalMarginBalance),
                      parseFloat(data.totalCrossWalletBalance),
                      parseFloat(data.maxWithdrawAmount),
                      parseFloat(data.totalUnrealizedProfit),
                      parseFloat(data.totalInitialMargin),
                      parseFloat(data.totalMaintMargin),
                      new Date()];
    const header2 = ["Asset", "Free", "Locked", "Total", "Margin Balance", "Cross Balance", "Max Withdraw", "UnPnl", "Initial Margin", "Maint. Margin"];
    const general = show_headers ? [header1, account, header2] : [];

    const assets = [];
    const balances = (data.assets || []).reduce(function(rows, a) {
      const asset = wallet.parseFuturesAsset(a);
      if (asset.initialMargin!==0 || asset.maintMargin!==0 || asset.unrealizedProfit!==0 || asset.walletBalance!==0 || asset.marginBalance!==0 || asset.crossWalletBalance!==0 || asset.availableBalance!==0 || asset.maxWithdrawAmount!==0) {
        // Only return assets with balance
        assets.push(asset);
        rows.push([
          asset.symbol,
          asset.free,
          asset.locked,
          asset.total,
          asset.marginBalance,
          asset.crossWalletBalance,
          asset.maxWithdrawAmount,
          asset.unrealizedProfit,
          asset.initialMargin,
          asset.maintMargin
        ]);
      }
      return rows;
    }, []);

    // Save futures assets to wallet
    wallet.setFuturesAssets(assets);

    return [...general, ...balances];
  }

  function parseFuturesPositions(data, show_headers) {
    const wallet = BinWallet();
    const header = ["Pair", "Side", "Leverage", "Entry", "Amount", "Notional", "UnPnl", "Isolated?", "Isolated Wallet", "Maint. Margin", "Initial Margin", "Position Initial Margin", "Open Orders Initial Margin"];
    const positions = (data.positions || []).reduce(function(rows, pos) {
      const position = wallet.parseFuturesPosition(pos);
      if (position.entry > 0) { // Only return positions with entry price
        rows.push([
          position.pair,
          position.side,
          position.leverage,
          position.entry,
          position.amount,
          position.notional,
          position.unpnl,
          position.isolated,
          position.isolatedWallet,
          position.maintMargin,
          position.initialMargin,
          position.positionInitialMargin,
          position.openOrderInitialMargin
        ]);
      }
      return rows;
    }, []);

    return show_headers ? [header, ...positions] : positions;
  }

  function parseSubAccounts(data, show_headers) {
    const wallet = BinWallet();
    const subaccs = BinSetup().getSubAccounts();
    const emails = Object.keys(subaccs);
    const headers = [
      ["Account Type", "Added Accounts", "Last Update"],
      ["Sub-Accounts", emails.length, new Date()]
    ];
    const general = show_headers ? headers : [];
    if (!emails.length) {
      general.push(["You have to add at least one sub-account email from 'Binance' main menu!"]);
    }

    const assets = [];
    const balances = Object.keys(data).reduce(function(rows, email) {
      const subbalances = (data[email]||[]).reduce(function(subrows, a) {
        const asset = wallet.parseSubAccountAsset(a);
        if (asset.total > 0) { // Only return assets with balance
          assets.push(asset);
          subrows.push([
            asset.symbol,
            asset.free,
            asset.locked,
            asset.total
          ]);
        }
        return subrows;
      }, []);

      if (show_headers) {
        rows.push(["Sub-Account Email", "", "", "Assets"]);
      }
      rows.push([email, "", "", subbalances.length]);
      if (show_headers) {
        rows.push(["Asset", "Free", "Locked", "Total"]);
      }

      const sorted = BinUtils().sortResults(subbalances);
      return rows.concat(sorted);
    }, []);

    // Save assets to wallet
    wallet.setSubAccountAssets(assets);

    return [...general, ...balances];
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    schedule,
    refresh,
    listSubAccounts,
    run
  };
}/**
 * Runs the 24h stats script.
 */
function BinDo24hStats() {
  let lock_retries = 5; // Max retries to acquire lock

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "stats/24h";
  }

  /**
   * Returns true if the given operation belongs to this module
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    return BinScheduler().getSchedule(tag()) || "30m";
  }
  
  /**
   * Returns the 24hs stats list against USDT.
   *
   * @param {["BTC","ETH"..]} range_or_cell If given, returns just the matching symbol price or range prices. If not given, returns all the prices.
   * @param options Ticker to match against (USDT by default) or an option list like "ticker: USDT, headers: false"
   * @return The list of 24hs stats for given symbols
   */
  function run(range_or_cell, options) {
    const bs = BinScheduler();
    try {
      bs.clearFailed(tag());
      return execute(range_or_cell, options);
    } catch(err) { // Re-schedule this failed run!
      bs.rescheduleFailed(tag());
      throw err;
    }
  }

  function execute(range_or_cell, options) {
    const ticker_against = options["ticker"];
    Logger.log("[BinDo24hStats] Running..");
    if (!range_or_cell) { // @TODO This limitation could be removed if cache is changed by other storage
      throw new Error("A range with crypto names must be given!");
    }
    const lock = BinUtils().getUserLock(lock_retries--);
    if (!lock) { // Could not acquire lock! => Retry
      return execute(range_or_cell, options);
    }
  
    const opts = {
      CACHE_TTL: 55,
      "public": true,
      "no_cache_ok": true,
      "validate_cache": function(data) {
        return BinUtils().checkExpectedResults(data, range_or_cell);
      },
      "filter": function(data) {
        return BinUtils().filterTickerSymbol(data, range_or_cell, ticker_against);
      }
    };
    const data = new BinRequest(opts).get("api/v3/ticker/24hr", "", "");
  
    BinUtils().releaseLock(lock);
    const parsed = parse(data, range_or_cell, options);
    Logger.log("[BinDo24hStats] Done!");
    return parsed;
  }

  function parse(data, range_or_cell, options) {
    const header = ["Date", "Symbol", "Price", "Ask", "Bid", "Open", "High", "Low", "Prev Close", "$ Change 24h", "% Change 24h", "Volume"];
    const parsed = data.reduce(function(rows, ticker) {
      if (ticker === "?") {
        rows.push(header.map(function() {
          return "?";
        }));
        return rows;
      }

      const symbol = ticker.symbol;
      const price = BinUtils().parsePrice(ticker.lastPrice);
      const ask_price = BinUtils().parsePrice(ticker.askPrice);
      const bid_price = BinUtils().parsePrice(ticker.bidPrice);
      const open_price = BinUtils().parsePrice(ticker.openPrice);
      const high_price = BinUtils().parsePrice(ticker.highPrice);
      const low_price = BinUtils().parsePrice(ticker.lowPrice);
      const close_price = BinUtils().parsePrice(ticker.prevClosePrice);
      const chg24h_price = parseFloat(ticker.priceChange);
      const chg24h_percent = parseFloat(ticker.priceChangePercent) / 100;
      const volume = parseFloat(ticker.quoteVolume);
      const row = [
        new Date(parseInt(ticker.closeTime)),
        symbol,
        price,
        ask_price,
        bid_price,
        open_price,
        high_price,
        low_price,
        close_price,
        chg24h_price,
        chg24h_percent,
        volume
      ];
      rows.push(row);
      return rows;
    }, []);

    const show_headers = BinUtils().parseBool(options.headers);
    if (range_or_cell) { // Return as it is if we have a cell or range to display
      return show_headers ? [header, ...parsed] : parsed;
    }
    // Return sorted results
    const sorted = BinUtils().sortResults(parsed, 1);
    return show_headers ? [header, ...sorted] : sorted;
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    run
  };
}/**
 * Runs the current prices script.
 */
function BinDoCurrentPrices() {
  let lock_retries = 5; // Max retries to acquire lock

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "prices";
  }

  /**
   * Returns true if the given operation belongs to this code
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    return BinScheduler().getSchedule(tag()) || "1m";
  }
  
  /**
   * Returns current market prices.
   *
   * @param {["BTC","ETH"..]} symbol_or_range If given, returns just the matching symbol price or range prices. If not given, returns all the prices.
   * @param options Ticker to match against (USDT by default) or an option list like "ticker: USDT, headers: false"
   * @return The list of current prices for all or given symbols/tickers.
   */
  function run(symbol_or_range, options) {
    const bs = BinScheduler();
    try {
      bs.clearFailed(tag());
      return execute(symbol_or_range, options);
    } catch(err) { // Re-schedule this failed run!
      bs.rescheduleFailed(tag());
      throw err;
    }
  }

  function execute(symbol_or_range, options) {
    Logger.log("[BinDoCurrentPrices] Running..");
    const lock = BinUtils().getUserLock(lock_retries--);
    if (!lock) { // Could not acquire lock! => Retry
      return execute(symbol_or_range, options);
    }

    const opts = {CACHE_TTL: 55, retries: 20, public: true};
    const data = new BinRequest(opts).get("api/v3/ticker/price", "", "");
    BinUtils().releaseLock(lock);
    const parsed = parse(data, symbol_or_range, options);
    Logger.log("[BinDoCurrentPrices] Done!");
    return parsed;
  }

  function parse(data, symbol_or_range, {ticker: ticker_against, headers: show_headers, prices: prices_only}) {
    prices_only = BinUtils().parseBool(prices_only, false);
    show_headers = BinUtils().parseBool(show_headers);
    const header = ["Symbol", "Price"];
    const tickers = symbol_or_range ? BinUtils().filterTickerSymbol(data, symbol_or_range, ticker_against) : data;
    if (typeof symbol_or_range == "string" && symbol_or_range) { // A single value to return
      return BinUtils().parsePrice(((tickers||[{}])[0]||{}).price);
    }

    // Multiple rows to return
    const parsed = tickers.reduce(function(rows, ticker) {
      const price = BinUtils().parsePrice(ticker.price);
      const row = prices_only ? price : [ticker.symbol, price];
      rows.push(row);
      return rows;
    }, []);

    if (prices_only) { // Return as it is if we only want to display prices
      return parsed;
    }
    if (symbol_or_range) { // Return as it is if we have a symbol or range to display
      return show_headers ? [header, ...parsed] : parsed;
    }
    // Return sorted results
    const sorted = BinUtils().sortResults(parsed);
    return show_headers ? [header, ...sorted] : sorted;
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    run
  };
}/**
 * Runs the done orders script.
 */
function BinDoOrdersDone() {
  const max_items = 100; // How many items to be displayed by default

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "orders/done";
  }

  /**
   * Returns true if the given operation belongs to this code
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    return BinScheduler().getSchedule(tag()) || "5m";
  }
  
  /**
   * Returns the most recent filled/done orders (100 by default) from ALL sheets that are "order tables" in the spreadsheet
   * NOTE: It requires at least ONE sheet with the 'orders/table' operation in it!
   *
   * @param {["BTC","ETH"..]} range_or_cell If given, will filter by given symbols (regexp).
   * @param options Ticker to match against (none by default) or an option list like "ticker: USDT, headers: false, max: 0"
   * @return A list of current done orders for given criteria.
   */
  function run(range_or_cell, options) {
    const bs = BinScheduler();
    try {
      bs.clearFailed(tag());
      return execute(range_or_cell, options);
    } catch(err) { // Re-schedule this failed run!
      bs.rescheduleFailed(tag());
      throw err;
    }
  }

  function execute(range_or_cell, options) {
    const ticker_against = options["ticker"] || "";
    Logger.log("[BinDoOrdersDone] Running..");

    const ot = BinDoOrdersTable();
    const has_sheets = ot.hasSheets();
    if (!has_sheets) {
      console.error("[BinDoOrdersDone] It seems that we didn't find any sheet in the spreadsheet with the 'orders/table' operation in it!");
      return [["ERROR: This operation requires at least ONE sheet in the spreadsheet with the 'orders/table' operation in it!"]];
    }

    // Get ALL the rows contained in ALL defined sheets as order tables!
    let data = ot.getRows();
    Logger.log("[BinDoOrdersDone] Found "+data.length+" orders to display.");
    const range = BinUtils().getRangeOrCell(range_or_cell);
    if (range.length) { // Apply filtering
      const pairs = range.map(symbol => new RegExp(symbol+ticker_against, "i"));
      data = data.filter(row => pairs.find(pair => pair.test(row[3])));
      Logger.log("[BinDoOrdersDone] Filtered to "+data.length+" orders.");
    }

    const parsed = data.length ? parse(data, options) : [["- no orders to display "+(range.length?"with given filters ":"")+"yet -"]];
    Logger.log("[BinDoOrdersDone] Done!");
    return parsed;
  }

  function parse(data, options) {
    const header = ["Order #ID", "Date", "Pair", "Type", "Side", "Price", "Amount", "Commission", "Total"];
    const parsed = data.map(function(order) {
      order.shift(); // Remove the first column (Trade #ID)
      return order;
    });
    let sorted = BinUtils().sortResults(parsed, 1, true);
    const limit = parseInt(options["max"]||max_items); // Max items to display
    if (limit > 0 && limit < sorted.length) {
      sorted = sorted.slice(0, limit);
    }
    return BinUtils().parseBool(options["headers"]) ? [header, ...sorted] : sorted;
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    run
  };
}/**
 * Runs the last update script.
 */
function BinDoLastUpdate() {
  const PROP_NAME = "BIN_LAST_UPDATE";
  const delay = 1000; // Delay getter calls in milliseconds

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "last_update";
  }

  /**
   * Returns true if the given operation belongs to this code
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    return BinScheduler().getSchedule(tag()) || "1m";
  }

  /**
  * Sets or returns the timestamp of the last issued request to Binance API.
  */
  function run(ts) {
    const doc_props = PropertiesService.getDocumentProperties();
    
    if (ts == undefined) { // Getter
      Utilities.sleep(delay); // Wait a little to try to get a fresh value (useful mainly when a trigger runs)
      const last_update = doc_props.getProperty(PROP_NAME);
      ts = last_update ? new Date(last_update) : "";
      Logger.log("[BinDoLastUpdate] Got last update time: "+ts);
      return ts;
    }
    
    // Setter
    ts = new Date();
    doc_props.setProperty(PROP_NAME, ts);
    Logger.log("[BinDoLastUpdate] Set last update time: "+ts);
    return ts;
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    run
  };
}/**
 * Runs the open orders script.
 */
function BinDoOrdersOpen() {
  let lock_retries = 5; // Max retries to acquire lock

  /**
   * Returns this function tag (the one that's used for BINANCE function 1st parameter)
   */
  function tag() {
    return "orders/open";
  }

  /**
   * Returns true if the given operation belongs to this code
   */
  function is(operation) {
    return operation === tag();
  }

  /**
   * Returns this function period (the one that's used by the refresh triggers)
   */
  function period() {
    return BinScheduler().getSchedule(tag()) || "5m";
  }
  
  /**
   * Returns current open oders.
   *
   * @param {["BTC","ETH"..]} range_or_cell If given, will filter by given symbols (regexp).
   * @param options Ticker to match against (none by default) or an option list like "ticker: USDT, headers: false"
   * @return A list of current open orders for given criteria.
   */
  function run(range_or_cell, options) {
    const bs = BinScheduler();
    try {
      bs.clearFailed(tag());
      return execute(range_or_cell, options);
    } catch(err) { // Re-schedule this failed run!
      bs.rescheduleFailed(tag());
      throw err;
    }
  }

  function execute(range_or_cell, options) {
    const ticker_against = options["ticker"] || "";
    Logger.log("[BinDoOrdersOpen] Running..");
    const lock = BinUtils().getUserLock(lock_retries--);
    if (!lock) { // Could not acquire lock! => Retry
      return execute(range_or_cell, options);
    }

    let data = fetch();
    BinUtils().releaseLock(lock);
    Logger.log("[BinDoOrdersOpen] Found "+data.length+" orders to display.");
    const range = BinUtils().getRangeOrCell(range_or_cell);
    if (range.length) { // Apply filtering
      const pairs = range.map(symbol => new RegExp(symbol+ticker_against, "i"));
      data = data.filter(row => pairs.find(pair => pair.test(row.symbol)));
      Logger.log("[BinDoOrdersOpen] Filtered to "+data.length+" orders.");
    }

    const parsed = parse(data, options);
    Logger.log("[BinDoOrdersOpen] Done!");
    return parsed;
  }

  function fetch() {
    const bw = BinWallet();
    const opts = {CACHE_TTL: 55, "discard_40x": true}; // Discard 40x errors for disabled wallets!
    const dataSpot = fetchSpotOrders(opts); // Get all SPOT orders
    const dataCross = bw.isEnabled("cross") ? fetchCrossOrders(opts) : []; // Get all CROSS MARGIN orders
    const dataIsolated = bw.isEnabled("isolated") ? fetchIsolatedOrders(opts) : []; // Get all ISOLATED MARGIN orders
    const dataFutures = bw.isEnabled("futures") ? fetchFuturesOrders(opts) : []; // Get all FUTURES orders (USD-M)
    const dataDelivery = bw.isEnabled("delivery") ? fetchDeliveryOrders(opts) : []; // Get all DELIVERY orders (COIN-M)
    return [...dataSpot, ...dataCross, ...dataIsolated, ...dataFutures, ...dataDelivery];
  }

  function fetchSpotOrders(opts) {
    Logger.log("[BinDoOrdersOpen][SPOT] Fetching orders..");
    const orders = new BinRequest(opts).get("api/v3/openOrders");
    return orders.map(function(order) {
      order.market = "SPOT";
      return order;
    });
  }

  function fetchCrossOrders(opts) {
    Logger.log("[BinDoOrdersOpen][CROSS] Fetching orders..");
    const orders = new BinRequest(opts).get("sapi/v1/margin/openOrders") || []; //  It may fail if wallet isn't enabled!
    return orders.map(function(order) {
      order.market = "CROSS";
      return order;
    });
  }

  function fetchIsolatedOrders(opts) {
    const wallet = BinWallet();
    const symbols = Object.keys(wallet.getIsolatedPairs());
    return symbols.reduce(function(acc, symbol) {
      Logger.log("[BinDoOrdersOpen][ISOLATED] Fetching orders for '"+symbol+"' pair..");
      const qs = "isIsolated=true&symbol="+symbol;
      const orders = new BinRequest(opts).get("sapi/v1/margin/openOrders", qs) || []; //  It may fail if wallet isn't enabled!
      const data = orders.map(function(order) {
        order.market = "ISOLATED";
        return order;
      });
      return [...acc, ...data];
    }, []);
  }

  function fetchFuturesOrders(opts) {
    Logger.log("[BinDoOrdersOpen][FUTURES USD-M] Fetching orders..");
    const options = Object.assign({futures: true}, opts);
    const orders = new BinRequest(options).get("fapi/v1/openOrders") || []; //  It may fail if wallet isn't enabled!
    return orders.map(function(order) {
      order.market = "FUTURES USD-M";
      return order;
    });
  }

  function fetchDeliveryOrders(opts) {
    Logger.log("[BinDoOrdersOpen][FUTURES COIN-M] Fetching orders..");
    const options = Object.assign({delivery: true}, opts);
    const orders = new BinRequest(options).get("dapi/v1/openOrders") || []; //  It may fail if wallet isn't enabled!
    return orders.map(function(order) {
      order.market = "FUTURES COIN-M";
      // Convert order.origQty that represent contracts amount
      order.origQty = Math.round(parseFloat(order.origQty) / _parseOrderPrice(order) * 100000000) / 100000000;
      return order;
    });
  }

  function parse(data, {headers: show_headers}) {
    const header = ["Date", "Pair", "Market", "Type", "Side", "Price", "Amount", "Executed", "Total"];
    const parsed = data.reduce(function(rows, order) {
      const symbol = order.symbol;
      const price = _parseOrderPrice(order);
      const amount = parseFloat(order.origQty);
      const row = [
        new Date(parseInt(order.time)),
        symbol,
        order.market,
        order.type,
        order.side,
        price,
        amount,
        parseFloat(order.executedQty),
        price*amount
      ];
      rows.push(row);
      return rows;
    }, []);

    const sorted = BinUtils().sortResults(parsed, 0, true);
    return BinUtils().parseBool(show_headers) ? [header, ...sorted] : sorted;
  }

  function _parseOrderPrice(order) {
    if (parseFloat(order.price)) {
      return BinUtils().parsePrice(order.price);
    }
    if (parseFloat(order.activatePrice)) { // Only returned for TRAILING_STOP_MARKET orders
      return BinUtils().parsePrice(order.activatePrice);
    }
    return BinUtils().parsePrice(order.stopPrice);
  }

  // Return just what's needed from outside!
  return {
    tag,
    is,
    period,
    run
  };
}/**
 * App config params.
 */

let DEBUG = false;
const VERSION = "v0.5.3";
const REPO_URL = "https://github.com/diegomanuel/binance-to-google-sheets";
const USE_PROXY = false;
// See: https://github.com/diegomanuel/binance-to-google-sheets-proxy
//const USE_PROXY = "https://btgs-proxy.setupme.io"
const SPOT_API_URL = "https://data.binance.com";
const FUTURES_API_URL = "https://fapi.binance.com";
const DELIVERY_API_URL = "https://dapi.binance.com";
const TICKER_AGAINST = "USDT";
const REQUEST_RETRY_MAX_ATTEMPTS = 10; // Max number of attempts when the API responses with status != 200
const REQUEST_RETRY_DELAY = 1000; // Delay between API calls when it fails in milliseconds
/**
 * API client wrapper.
 */
function BinRequest(OPTIONS) {
  OPTIONS = OPTIONS || {}; // Init options
  const CACHE_TTL = OPTIONS["CACHE_TTL"] || false; // Cache disabled by default
  const retry_max_attempts = REQUEST_RETRY_MAX_ATTEMPTS; // Max number of attempts when the API responses with status != 200
  const retry_delay = REQUEST_RETRY_DELAY; // Delay between API calls when it fails in milliseconds

  return {
    get
  };

  /**
   * Reads data from cache or Binance API with a GET request
   */
  function get(url, qs, payload) {
    return _fetch("get", url, qs, payload);
  }

  /**
   * Reads data from cache or Binance API
   */
  function _fetch(method, url, qs, payload) {
    if (!OPTIONS["CACHE_TTL"]) { // Cache is disabled on purpose for this call
      return _request(method, url, qs, payload, OPTIONS); // Send request to Binance API
    }
    return _cache(method, url, qs, payload); // Maybe cache or Binance API
  }

  /**
  * Reads data from cache or sends a request to Binance API and stores the data into cache with given TTL.
  */
  function _cache(method, url, qs, payload) {
    const CACHE_KEY = method+"_"+url+"_"+qs;
    let data = BinCache().read(CACHE_KEY, OPTIONS["validate_cache"]);

    // Check if we have valid cached data
    if (!(data && Object.keys(data).length)) { // Fetch data from API
      Logger.log("NO CACHE entry found! Loading data from API..");
      data = _request(method, url, qs, payload, OPTIONS);
      if (data && Object.keys(data).length && OPTIONS["filter"]) {
        data = OPTIONS["filter"](data); // Apply custom data filtering before storing into cache
      }
      if (data && Object.keys(data).length) {
        Logger.log("DONE loading data from API! Storing at cache..");
        BinCache().write(CACHE_KEY, data, CACHE_TTL);
      } else {
        Logger.log("DONE loading data from API, but NO results to return!");
      }
    } else {
      Logger.log("FOUND CACHE entry!");
      if (OPTIONS["filter"]) { // Apply custom data filtering before return it
        data = OPTIONS["filter"](data);
      }
    }
    
    return data;
  }

  /**
  * Sends a request to Binance API with given parameters.
  */
  function _request(method, url, qs, payload, opts) {
    const CACHE_OK_KEY = method+"_"+url+"_"+qs;
    const API_URL = _makeApiUrl(opts);
    const need_auth = !opts["public"]; // Calling a private endpoint
    const headers = opts["headers"] || {};
    const da_payload = payload ? JSON.stringify(payload) : "";
    let da_qs = qs || "";
    let options = {
      "method": method,
      "contentType": "application/json",
      "headers": headers,
      "payload": da_payload,
      "muteHttpExceptions": true,
      "validateHttpsCertificates": true
    };

    if (need_auth) { // Calling a private endpoint
      if (!BinSetup().areAPIKeysConfigured()) { // Do not allow to continue if API keys aren't set!
        throw new Error("Binance API keys are required to call this operation!");
      }
      options["headers"]["X-MBX-APIKEY"] = BinSetup().getAPIKey();
      da_qs += (da_qs?"&":"")+"timestamp="+(new Date()).getTime()+"&recvWindow=30000";
      da_qs += "&signature="+_computeSignature(da_qs, da_payload);
    }
    const da_url = API_URL+"/"+url+"?"+da_qs;
    const response = UrlFetchApp.fetch(da_url, options);
    if (DEBUG) {
      Logger.log("QUERY: "+da_url);
      Logger.log("RESPONSE: "+response.getResponseCode());
    }
    if (response.getResponseCode() == 200) {
      BinDoLastUpdate().run(new Date()); // Refresh last update ts
      const data = JSON.parse(response.getContentText() || "{}");
      if (!opts["no_cache_ok"]) { // Keep last OK response
        _setLastCacheResponseOK(CACHE_OK_KEY, da_payload, data);
      }
      return data; 
    }
    if (response.getResponseCode() == 400) {
      // There might be a problem with the Binance API keys
      if (opts["discard_40x"]) {
        return null; // Return a null response when discarding 40x errors!
      }
      throw new Error("Got 400 from Binance API! The request seems to be wrong.");
    }
    if (response.getResponseCode() == 401) {
      // There might be a problem with the Binance API keys
      if (opts["discard_40x"]) {
        return null; // Return a null response when discarding 40x errors!
      }
      throw new Error("Got 401 from Binance API! The keys aren't set or they are not valid anymore.");
    }
    if (response.getResponseCode() == 418) {
      // The IP has been auto-banned for continuing to send requests after receiving 429 codes
      Logger.log("Got 418 from Binance API! We are banned for a while..  =/");
      const options = _canRetryRequest(418, opts);
      if (options) { // Somewhat "weird" function, it acts as a bool helper and opts updater at once.. but whatever..!
        return _request(method, url, qs, payload, options);
      }
    }
    if (response.getResponseCode() == 429) {
      // Binance is telling us that we are sending too many requests
      Logger.log("Got 429 from Binance API! We are sending too many requests from our IP..  =/");
      const options = _canRetryRequest(429, opts);
      if (options) { // Somewhat "weird" function, it acts as a bool helper and opts updater at once.. but whatever..!
        return _request(method, url, qs, payload, options);
      }
    }

    if (!opts["no_cache_ok"]) { // Fallback to last cached OK response data (if any)
      const cached_data = _getLastCacheResponseOK(CACHE_OK_KEY, da_payload);
      if (cached_data && Object.keys(cached_data).length) {
        Logger.log("Couldn't get an OK response from Binance API! Fallback to last cached OK response data..  =0");
        return cached_data;
      }
    }

    throw new Error("Request failed with status: "+response.getResponseCode());
  }

  function _makeApiUrl(opts) {
    if (USE_PROXY) {
      return _makeProxyApiUrl(opts)
    }
    if (opts["futures"]) {
      return FUTURES_API_URL;
    }
    if (opts["delivery"]) {
      return DELIVERY_API_URL;
    }

    /**
    * Builds an URL for the Spot API, using one of the 4 available clusters at random.
    * Thank you @fabiob for the PR! :: https://github.com/fabiob
    * @see {@link https://binance-docs.github.io/apidocs/spot/en/#general-api-information}
    */
    return SPOT_API_URL.replace(/api/, `api${Math.floor(Math.random() * 4) || ''}`);
  }

  // The ports below should match the ones defined at:
  // https://github.com/diegomanuel/binance-to-google-sheets-proxy/blob/main/config/config.exs
  function _makeProxyApiUrl(opts) {
    if (opts["futures"]) {
      return USE_PROXY+":4004";
    }
    if (opts["delivery"]) {
      return USE_PROXY+":4005";
    }
    // Ports 4000 to 4003
    return `${USE_PROXY}:400${Math.floor(Math.random() * 4) || '0'}`;
  }

  /**
   * Retries an execution for given status code.
   */
  function _canRetryRequest(code, opts) {
    const max_attempts = Math.max(opts["retries"]||0, retry_max_attempts);
    if ((opts["retries_"+code]||0) < max_attempts) {
      opts["retries_"+code] = (opts["retries_"+code]||0) + 1;
      Logger.log("Retry "+opts["retries_"+code]+"/"+max_attempts+" for status code ["+code+"] in "+retry_delay+" milliseconds..");
      Utilities.sleep(retry_delay); // Wait a little and try again!
      return opts;
    }

    return false;
  }

  /**
  * Sets last OK response into cache.
  */
  function _setLastCacheResponseOK(qs, payload, data) {
    const CACHE_TTL = 60 * 60; // 1 hour, in seconds
    Logger.log("[cache-OK] Saving OK response to cache for "+CACHE_TTL+" seconds.");
    return BinCache().write("OK_"+qs+"_"+payload, data, CACHE_TTL);
  }

  /**
  * Gets last OK response from cache.
  */
  function _getLastCacheResponseOK(qs, payload) {
    Logger.log("[cache-OK] Getting OK response from cache.");
    return BinCache().read("OK_"+qs+"_"+payload);
  }

  /**
  * Computes the HMAC signature for given query string.
  */
  function _computeSignature(qs, payload) {
    const secret = BinSetup().getAPISecret();
    return Utilities
      .computeHmacSha256Signature(qs+(payload||""), secret)
      .reduce(function(str, chr) {
        chr = (chr < 0 ? chr + 256 : chr).toString(16);
        return str + (chr.length===1?'0':'') + chr;
      },'');
  }
}
/**
 * Account wallet wrapper.
 */
function BinWallet(OPTIONS) {
  OPTIONS = OPTIONS || {}; // Init options
  const WALLET_PROP_NAME = "BIN_ACCOUNT_WALLET";

  return {
    isEnabled,
    getAssets,
    getSpotAssets,
    getCrossAssets,
    getIsolatedAssets,
    getFuturesAssets,
    getDeliveryAssets,
    getSubAccountAssets,
    setSpotAssets,
    setCrossAssets,
    setIsolatedAssets,
    setFuturesAssets,
    setDeliveryAssets,
    setSubAccountAssets,
    getIsolatedPairs,
    setIsolatedPairs,
    parseSpotAsset,
    parseCrossMarginAsset,
    parseIsolatedMarginAsset,
    parseFuturesAsset,
    parseFuturesPosition,
    parseSubAccountAsset,
    refreshAssets,
    clearAssets,
    calculateAssets
  };

  /**
   * Returns true if the given wallet type is currently enabled
   */
  function isEnabled(type) {
    const wallets = BinSetup().getDisabledWallets();
    return !wallets[type];
  }

  /**
   * Returns the account wallet assets for SPOT
   */
  function getSpotAssets(symbol) {
    return getAssets("spot", symbol);
  }

  /**
   * Returns the account wallet assets for CROSS MARGIN
   */
  function getCrossAssets(symbol) {
    return getAssets("cross", symbol);
  }

  /**
   * Returns the account wallet assets for ISOLATED MARGIN
   */
  function getIsolatedAssets(symbol) {
    return getAssets("isolated", symbol);
  }

  /**
   * Returns the account wallet assets for FUTURES USD-M
   */
  function getFuturesAssets(symbol) {
    return getAssets("futures", symbol);
  }

  /**
   * Returns the account wallet assets for FUTURES COIN-M
   */
  function getDeliveryAssets(symbol) {
    return getAssets("delivery", symbol);
  }

  /**
   * Returns the account wallet assets for SUB-ACCOUNTS
   */
  function getSubAccountAssets(symbol) {
    return getAssets("sub-accounts", symbol);
  }

  function getAssets(type, symbol) {
    const data = PropertiesService.getScriptProperties().getProperty(WALLET_PROP_NAME+"_"+type.toUpperCase());
    const assets = data ? JSON.parse(data) : {};
    return symbol ? assets[symbol] : assets;
  }

  /**
   * Sets account wallet data for SPOT
   */
  function setSpotAssets(data) {
    return setAssetsData("spot", data);
  }

  /**
   * Sets account wallet data for CROSS MARGIN
   */
  function setCrossAssets(data) {
    return setAssetsData("cross", data);
  }

  /**
   * Sets account wallet data for ISOLATED MARGIN
   */
  function setIsolatedAssets(data) {
    return setAssetsData("isolated", data);
  }

  /**
   * Sets account wallet data for FUTURES USD-M
   */
  function setFuturesAssets(data) {
    return setAssetsData("futures", data);
  }

  /**
   * Sets account wallet data for FUTURES COIN-M
   */
  function setDeliveryAssets(data) {
    return setAssetsData("delivery", data);
  }

  /**
   * Sets account wallet data for SUB-ACCOUNTS
   */
  function setSubAccountAssets(data) {
    return setAssetsData("sub-accounts", data);
  }

  function setAssetsData(type, data) {
    Logger.log("[BinWallet] Updating wallet assets for: "+type.toUpperCase());
    const assets = data.reduce(function(acc, asset) {
      return _accAssetHelper(acc, asset.symbol, asset);
    }, {});

    return PropertiesService.getScriptProperties()
      .setProperty(WALLET_PROP_NAME+"_"+type.toUpperCase(), JSON.stringify(assets));
  }

  /**
   * Gets pairs data for ISOLATED MARGIN
   */
  function getIsolatedPairs(symbol) {
    const data = PropertiesService.getScriptProperties().getProperty(WALLET_PROP_NAME+"_ISOLATED_PAIRS");
    const pairs = data ? JSON.parse(data) : {};
    return symbol ? pairs[symbol] : pairs;
  }

  /**
   * Sets pairs data for ISOLATED MARGIN
   */
  function setIsolatedPairs(data) {
    const pairs = data.reduce(function(acc, pair) {
      acc[pair.symbol] = pair;
      return acc;
    }, {});

    return PropertiesService.getScriptProperties()
      .setProperty(WALLET_PROP_NAME+"_ISOLATED_PAIRS", JSON.stringify(pairs));
  }

  function parseSpotAsset(asset) {
    const free = parseFloat(asset.free);
    const locked = parseFloat(asset.locked);
    return {
      symbol: asset.asset,
      free,
      locked,
      borrowed: 0,
      interest: 0,
      total: free + locked,
      net: free + locked,
      netBTC: 0 // Missing!
    };
  }

  function parseCrossMarginAsset(asset) {
    const free = parseFloat(asset.free);
    const locked = parseFloat(asset.locked);
    return {
      symbol: asset.asset,
      free,
      locked,
      borrowed: parseFloat(asset.borrowed),
      interest: parseFloat(asset.interest),
      total: free + locked,
      net: parseFloat(asset.netAsset),
      netBTC: 0 // Missing!
    };
  }

  function parseIsolatedMarginAsset(asset) {
    return {
      symbol: asset.asset,
      free: parseFloat(asset.free),
      locked: parseFloat(asset.locked),
      borrowed: parseFloat(asset.borrowed),
      interest: parseFloat(asset.interest),
      total: parseFloat(asset.totalAsset),
      net: parseFloat(asset.netAsset),
      netBTC: parseFloat(asset.netAssetOfBtc)
    };
  }

  function parseFuturesAsset(asset) {
    const total = parseFloat(asset.walletBalance);
    const free = parseFloat(asset.availableBalance);
    return {
      symbol: asset.asset,
      free,
      locked: total - free,
      borrowed: 0,
      interest: 0,
      total,
      net: total,
      netBTC: 0,
      initialMargin: parseFloat(asset.initialMargin),
      maintMargin: parseFloat(asset.maintMargin),
      unrealizedProfit: parseFloat(asset.unrealizedProfit),
      walletBalance: total,
      marginBalance: parseFloat(asset.marginBalance),
      crossWalletBalance: parseFloat(asset.crossWalletBalance),
      availableBalance: free,
      maxWithdrawAmount: parseFloat(asset.maxWithdrawAmount)
    };
  }

  function parseFuturesPosition(position) {
    return {
      pair: position.symbol,
      side: position.positionSide,
      leverage: parseFloat(position.leverage),
      entry: parseFloat(position.entryPrice),
      amount: parseFloat(position.positionAmt),
      notional: parseFloat(position.notional),
      maxNotional: parseFloat(position.maxNotional),
      unpnl: parseFloat(position.unrealizedProfit),
      isolated: position.isolated,
      isolatedWallet: parseFloat(position.isolatedWallet),
      initialMargin: parseFloat(position.initialMargin),
      maintMargin: parseFloat(position.maintMargin),
      positionInitialMargin: parseFloat(position.positionInitialMargin),
      openOrderInitialMargin: parseFloat(position.openOrderInitialMargin)
    };
  }

  function parseSubAccountAsset(asset) {
    const free = parseFloat(asset.free);
    const locked = parseFloat(asset.locked);
    return {
      symbol: asset.asset,
      free,
      locked,
      borrowed: 0,
      interest: 0,
      total: free + locked,
      net: free + locked,
      netBTC: 0 // Missing!
    };
  }

  /**
   * Refreshes the assets from ALL supported/available wallets
   */
  function refreshAssets(exclude_sub_accounts) {
    return BinDoAccountInfo().refresh(exclude_sub_accounts);
  }

  /**
   * Clears all the assets for the given wallet type
   */
  function clearAssets(type) {
    return setAssetsData(type, []);
  }

  /**
   * Returns a summary object whose keys are the asset name/symbol
   * and the values are the sum of each asset from all implemented wallets
   */
  function calculateAssets(exclude_sub_accounts) {
    let totals = {};
    
    const spot = getSpotAssets();
    totals = Object.keys(spot).reduce(function(acc, symbol) {
      return _accAssetHelper(acc, symbol, spot[symbol]);
    }, totals);
    if (isEnabled("cross")) {
      const cross = getCrossAssets();
      totals = Object.keys(cross).reduce(function(acc, symbol) {
        return _accAssetHelper(acc, symbol, cross[symbol]);
      }, totals);
    }
    if (isEnabled("isolated")) {
      const isolated = getIsolatedAssets();
      totals = Object.keys(isolated).reduce(function(acc, symbol) {
        return _accAssetHelper(acc, symbol, isolated[symbol]);
      }, totals);
    }
    if (isEnabled("futures")) {
      const futures = getFuturesAssets();
      totals = Object.keys(futures).reduce(function(acc, symbol) {
        return _accAssetHelper(acc, symbol, futures[symbol]);
      }, totals);
    }
    if (isEnabled("delivery")) {
      const delivery = getDeliveryAssets();
      totals = Object.keys(delivery).reduce(function(acc, symbol) {
        return _accAssetHelper(acc, symbol, delivery[symbol]);
      }, totals);
    }
    if (!exclude_sub_accounts) { // Include sub-account assets
      const subaccs = getSubAccountAssets();
      totals = Object.keys(subaccs).reduce(function(acc, symbol) {
        return _accAssetHelper(acc, symbol, subaccs[symbol]);
      }, totals);
    }

    return totals;
  }

  function _accAssetHelper(acc, symbol, asset) {
    acc[symbol] = {
      free: (asset.free||0) + (acc[symbol] ? acc[symbol].free : 0),
      locked: (asset.locked||0) + (acc[symbol] ? acc[symbol].locked : 0),
      borrowed: (asset.borrowed||0) + (acc[symbol] ? acc[symbol].borrowed : 0),
      interest: (asset.interest||0) + (acc[symbol] ? acc[symbol].interest : 0),
      total: (asset.total||0) + (acc[symbol] ? acc[symbol].total : 0),
      net: (asset.net||0) + (acc[symbol] ? acc[symbol].net : 0),
      netBTC: (asset.netBTC||0) + (acc[symbol] ? acc[symbol].netBTC : 0)
    };
    return acc;
  }
}/**
 * General utility functions wrapper.
 */
function BinUtils() {
  let lock_retries = 5; // Max retries to acquire lock for formulas refreshing

  return {
    releaseLock,
    getDocumentLock,
    getScriptLock,
    getUserLock,
    getRangeOrCell,
    parsePrice,
    parseBool,
    parseDate,
    parseOptions,
    checkExpectedResults,
    filterTickerSymbol,
    sortResults,
    obscureSecret,
    isFormulaMatching,
    extractFormulaParams,
    forceRefreshSheetFormulas,
    refreshMenu,
    toast
  };
  
  /**
  * Releases a lock (failsafe).
  */
  function releaseLock(lock) {
    try {
      return lock ? lock.releaseLock() : false;
    } catch (err) {
      console.error("Can't release lock: "+JSON.stringify(err));
    }
  }

  /**
  * Gets a lock that prevents any user of the current document from concurrently running a section of code.
  */
  function getDocumentLock(retries, time, sleep) {
    return _getLock("getDocumentLock", retries, time, sleep);
  }

  /**
  * Gets a lock that prevents any user from concurrently running a section of code.
  */
  function getScriptLock(retries, time, sleep) {
    return _getLock("getScriptLock", retries, time, sleep);
  }

  /**
  * Gets a lock that prevents the current user from concurrently running a section of code.
  */
  function getUserLock(retries, time, sleep) {
    return _getLock("getUserLock", retries, time, sleep);
  }

  /**
  * Gets lock, waiting for given `time` to acquire it, or sleep given `sleep` milliseconds to return `false`.
  */
  function _getLock(lock_serv_func, retries, time, sleep) {
    time = time || 5000; // Milliseconds
    sleep = sleep || 500; // Milliseconds
    const lock = LockService[lock_serv_func]();
    try {
      if (!lock.tryLock(time) || !lock.hasLock()) {
        throw new Error("=["); // Couldn't acquire lock!
      }
    } catch(_) { // Couldn't acquire lock!
      if (retries > 0) { // We still have retries left
        Logger.log("["+retries+"] Could not acquire lock! Waiting "+sleep+"ms and retrying..");
        Utilities.sleep(sleep);
        return false;
      }
      throw new Error("Could not acquire lock! Retries depleted, giving up..  =[");
    }
    return lock;
  }

  /**
   * Always returns an array no matter if it's a single cell or an entire range
   */
  function getRangeOrCell(range_or_cell, sheet) {
    if (typeof range_or_cell !== "string") {
      return Array.isArray(range_or_cell) ? range_or_cell : (range_or_cell ? [range_or_cell] : []);
    }

    const parseValues = () => (range_or_cell||"").split(",").map(s => s.trim()).filter(s => s.length);
    try {
      return sheet ? sheet.getRange(range_or_cell).getValues() : parseValues();
    } catch (_) {
      return parseValues();
    }
  }

  /**
   * Returns a given price as a float number or "?" if it's wrong
   * NOTE: Only makes sense to pass prices > 0 because a $0 price will produce "?"
   * And besides.. nothing worths $0 right? nothing's free! (except this script, of course =)
   */
  function parsePrice(price) {
    return (parseFloat(price) || "?");
  }

  /**
   * Returns a boolean for given value
   */
  function parseBool(val, default_val) {
    return val === default_val || val === true || val === 1 || val === "1" || val === "true" || val === "yes" || val === "y";
  }

  /**
   * Returns a date object from given string with format: YYYY-MM-DD and/or YYYY-MM-DD-HH-MM-SS
   */
  function parseDate(val) {
    const parts = (val||"").split("-");
    const date = parts[0] && parts[1] && parts[2] ? parts[0]+"-"+parts[1]+"-"+parts[2] : "";
    const time = parts.length > 3 ? (parts[3]||"00")+":"+(parts[4]||"00")+":"+(parts[5]||"00") : "";
    return new Date(date + (time ? " "+time : ""));
  }

  /**
   * Returns the options parsed from a string like "ticker: USDT, headers: false"
   * or a single value like "USDT" as {ticker: "USDT"}
   */
  function parseOptions(opts_or_value) {
    const matches = (opts_or_value||"").matchAll(/([^,\s]+):\s*([^,\s]+)/ig);
    const marr = [...matches];
    const options = marr.reduce(function(obj, [_, key, val]) {
      obj[key] = val;
      return obj;
    }, {ticker: marr.length ? undefined : (opts_or_value||undefined)});

    if (DEBUG) {
      Logger.log("PARSE OPTS MATCHES: "+JSON.stringify(marr));
    }

    return options;
  }

  /**
   * Checks if given data array has the expected values from given range_or_cell (just their length)
   * @param data Array with tickers data
   * @param range_or_cell A range of cells or a single cell
   */
  function checkExpectedResults(data, range_or_cell) {
    return (data||[]).length === (getRangeOrCell(range_or_cell)||[]).length;
  }

  /**
   * Filters a given data array by given range of values or a single value
   * @param data Array with tickers data
   * @param range_or_cell A range of cells or a single cell
   * @param ticker_against Ticker to match against
   */
  function filterTickerSymbol(data, range_or_cell, ticker_against) {
    ticker_against = ticker_against || TICKER_AGAINST;
    const cryptos = getRangeOrCell(range_or_cell);
    const tickers = cryptos.reduce(function(tickers, crypto) { // Init expected tickers to be returned
        tickers[crypto+ticker_against] = "?";
        return tickers;
      }, {});

    const data_array = Array.isArray(data) ? data : (data ? [data] : []);
    const results = data_array.reduce(function(tickers, ticker) {
      if (tickers[ticker.symbol] !== undefined) { // This ticker is one of the expected ones
        tickers[ticker.symbol] = ticker;
      }
      return tickers;
      }, tickers);

    return Object.values(results);
  }
  
  /**
  * Sorts a results array by given index (default 0) and direction (default ASC)
  */
  function sortResults(results, index, reverse) {
    return (results||[]).sort(function(v1, v2) {
      return (v1[index||0] > v2[index||0] ? 1 : -1) * (reverse ? -1 : 1);
    });
  }
  
  /**
  * Replaces some characters to obscure the given secret.
  */
  function obscureSecret(secret) {
    if (!(secret && secret.length)) {
      return "";
    }

    const length = 20;
    const start = parseInt(secret.length / 2) - parseInt(length / 2);
    return secret.substr(0,start)+"*".repeat(length-1)+secret.substr(start+length);
  }

  /**
   * Returns true/false if the given period and formula matches the given module
   */
  function isFormulaMatching(module, period, formula) {
    const regex_formula = "=.*BINANCE[R]?\\s*\\(\\s*\""+module.tag()+"\"";
    return module.tag() == period || (module.period() == period && new RegExp(regex_formula, "i").test(formula));
  }

  /**
   * Extract parameters from the formula string for the given module
   */
  function extractFormulaParams(module, formula) {
    const base_regex = "=.*BINANCE[R]?\\s*\\(\\s*\""+module.tag()+"\"\\s*,\\s*";
    let [range_or_cell, options] = ["", ""];

    // 3 params formula with string 2nd param
    let regex_formula = base_regex+"\"(.*)\"\\s*,\\s*\"(.*)\"";
    let extracted = new RegExp(regex_formula, "ig").exec(formula);
    if (extracted && extracted[1] && extracted[2]) {
      range_or_cell = extracted[1];
      options = extracted[2];
    } else {
      // 3 params formula with NOT-string 2nd param
      regex_formula = base_regex+"(.*)\\s*,\\s*\"(.*)\"";
      extracted = new RegExp(regex_formula, "ig").exec(formula);
      if (extracted && extracted[1] && extracted[2]) {
        range_or_cell = extracted[1];
        options = extracted[2];
      } else {
        // 2 params formula with string 2nd param
        regex_formula = base_regex+"\"(.*)\"";
        extracted = new RegExp(regex_formula, "ig").exec(formula);
        if (extracted && extracted[1]) {
          range_or_cell = extracted[1];
        } else {
          // 2 params formula with NOT-string 2nd param
          regex_formula = base_regex+"(.*)\\s*\\)";
          extracted = new RegExp(regex_formula, "ig").exec(formula);
          if (extracted && extracted[1]) {
            range_or_cell = extracted[1];
          }
        }
      }
    }

    if (DEBUG) {
      Logger.log("FORMULA: "+formula);
      if (extracted) {
        extracted.map(function(val) {
          Logger.log("REGEXP VAL: "+val);
        });
      }
      Logger.log("RANGE OR CELL: "+range_or_cell);
      Logger.log("OPTIONS: "+options);
    }

    return [range_or_cell, parseOptions(options)];
  }

  /**
   * Force-refresh formulas for given period.
   * NOTE: The period can also be a module tag!
   */
  function forceRefreshSheetFormulas(period) {
    let lock = null;

    Logger.log("Refreshing spreadsheet formulas..");
    if (!period) { // Just use lock if we are going to refresh ALL formulas!
      lock = getScriptLock(lock_retries--);
      if (!lock) { // Could not acquire lock! => Retry
        return forceRefreshSheetFormulas(period);
      }
    }

    const sheets = SpreadsheetApp.getActiveSpreadsheet().getSheets();
    const affected_formula_cells = sheets.reduce(function(formulas, sheet) {
      const formula_cells = _findAffectedSheetFormulaCells(period, sheet);
      return formulas.concat(formula_cells); // Add formulas to be refreshed (if any)
    }, []);
    if (DEBUG) {
      Logger.log("FORMULAS TO REFRESH: "+JSON.stringify(affected_formula_cells));
    }

    const regex_is_formula_refresh = new RegExp("BINANCER\\(", "i");
    for (const cell of affected_formula_cells) {
      const formula = cell.getFormula();
      if (formula.match(regex_is_formula_refresh)) {
        cell.setFormula(formula.replace(/BINANCER\(/i, "BINANCE("));
      } else {
        cell.setFormula(formula.replace(/BINANCE\(/i, "BINANCER("));
      }
    }

    releaseLock(lock);
    Logger.log(affected_formula_cells.length+" spreadsheet formulas were refreshed!");
    return affected_formula_cells.length;
  }

  function _findAffectedSheetFormulaCells(period, sheet) {
    let affected_formula_cells = [];

    try { // [#13] It may fail when a chart is moved to its own sheet!
      const range = sheet.getDataRange();
      const formulas = range.getFormulas();
      const num_cols = range.getNumColumns();
      const num_rows = range.getNumRows();
      for (let row = 0; row < num_rows ; row++) {
        for (let col = 0; col < num_cols; col++) {
          if (_isFormulaReplacement(period, formulas[row][col])) {
            const row_offset = range.getRow();
            const col_offset = range.getColumn();
            affected_formula_cells.push(range.getCell(row+row_offset, col+col_offset));
          }
        }
      }
    } catch(_) {}

    return affected_formula_cells;
  }

  function _isFormulaReplacement(period, formula) {
    if (!(formula != "" && new RegExp(/=.*BINANCE[R]?\s*\(/).test(formula))) {
      return false;
    }
    
    return  !period
              ||
            isFormulaMatching(BinDoCurrentPrices(), period, formula)
              ||
            isFormulaMatching(BinDoHistoricalData(), period, formula)
              ||
            isFormulaMatching(BinDo24hStats(), period, formula)
              ||
            isFormulaMatching(BinDoOrdersDone(), period, formula)
              ||
            isFormulaMatching(BinDoOrdersOpen(), period, formula)
              ||
            isFormulaMatching(BinDoAccountInfo(), period, formula)
              ||
            isFormulaMatching(BinDoLastUpdate(), period, formula);
          
  }

  /**
   * Refreshes "Binance" main menu items
   * @TODO This one should be at `BinMenu`
   */
  function refreshMenu() {
    return BinMenu(SpreadsheetApp.getUi());
  }

  /**
   * Displays a toast message on screen
   */
  function toast(body, title, timeout) {
    return SpreadsheetApp.getActive().toast(
      body,
      title || "Binance to Google Sheets",
      timeout || 10 // In seconds
    );
  }
}
/**
 * Scheduler coordinator wrapper.
 */
function BinScheduler(OPTIONS) {
  OPTIONS = OPTIONS || {}; // Init options
  const SCHEDULES_PROP_NAME = "BIN_SCHEDULER_ENTRIES";
  const RESCHEDULES_PROP_NAME = "BIN_SCHEDULER_ENTRIES_RETRY";
  const LAST_RUN_PROP_NAME = "BIN_SCHEDULER_LAST_RUN";

  return {
    init,
    run1m,
    run5m,
    run10m,
    run15m,
    run30m,
    run60m,
    getSchedule,
    setSchedule,
    cleanSchedules,
    rescheduleFailed,
    clearFailed,
    isStalled
  };

  /**
   * Mark the scheduler as initialized (ugly workaround)
   */
  function init() {
    _updateLastRun();
  }

  /**
   * Runs the scheduled functions for 1m
   */
  function run1m() {
    _updateLastRun();
    BinDoOrdersTable().init(); // Initialize orders table sheets (if any)
    BinUtils().forceRefreshSheetFormulas("1m");
  }

  /**
   * Runs the scheduled functions for 5m
   */
  function run5m() {
    _updateLastRun();
    BinUtils().forceRefreshSheetFormulas("5m");
  }

  /**
   * Runs the scheduled functions for 10m
   */
  function run10m() {
    _updateLastRun();
    BinUtils().forceRefreshSheetFormulas("10m");
  }

  /**
   * Runs the scheduled functions for 15m
   */
  function run15m() {
    _updateLastRun();
    BinUtils().forceRefreshSheetFormulas("15m");
  }

  /**
   * Runs the scheduled functions for 30m
   */
  function run30m() {
    _updateLastRun();
    BinUtils().forceRefreshSheetFormulas("30m");
  }

  /**
   * Runs the scheduled functions for 60m
   */
  function run60m() {
    _updateLastRun();
    BinUtils().forceRefreshSheetFormulas("1h");
  }

  /**
   * Returns the scheduled interval for given operation (or all schedules if no operation given)
   */
  function getSchedule(operation) {
    const rescheduled = _getRescheduled();
    if (operation && rescheduled[operation]) { // This operation failed before and was re-sheduled!
      return rescheduled[operation];
    }
    const props = _getDocPropService().getProperty(SCHEDULES_PROP_NAME);
    const schedules = props ? JSON.parse(props) : {};
    return operation ? schedules[operation] : schedules;
  }

  /**
   * Sets the scheduled interval for given operation
   */
  function setSchedule(operation, interval) {
    const schedules = getSchedule(); // Get all current schedules
    schedules[operation] = interval; // Set the given schedule
    Logger.log("Setting new schedule for ["+operation+"] at: "+interval);
    Logger.log("Updated schedules: "+JSON.stringify(schedules));
    return _getDocPropService().setProperty(SCHEDULES_PROP_NAME, JSON.stringify(schedules));
  }

  /**
   * Cleans ALL the scheduled intervals
   */
  function cleanSchedules() {
    Logger.log("Cleaning ALL schedules:\n"+JSON.stringify(getSchedule()));
    return _getDocPropService().setProperty(SCHEDULES_PROP_NAME, JSON.stringify({}));
  }

  /**
   * Re-schedule failed execution for given operation so it can be retried ASAP (at 1m trigger)
   */
  function rescheduleFailed(operation) {
    const reschedules = _getRescheduled(); // Get all current re-scheduled operations
    reschedules[operation] = "1m"; // Retry this operation at 1m trigger!
    Logger.log("Setting new retry schedule for: "+operation);
    Logger.log("Updated re-schedules: "+JSON.stringify(reschedules));
    return _getDocPropService().setProperty(RESCHEDULES_PROP_NAME, JSON.stringify(reschedules));
  }

  /**
   * Clears failed execution schedule for given operation (if any)
   * NOTE: This function could cause problems on parallel executions!
   */
  function clearFailed(operation) {
    const reschedules = _getRescheduled(); // Get all current re-scheduled operations
    if (reschedules[operation]) {
      delete reschedules[operation]; // Clear this operation!
      Logger.log("Clearing retry schedule for: "+operation);
      Logger.log("Updated re-schedules: "+JSON.stringify(reschedules));
      return _getDocPropService().setProperty(RESCHEDULES_PROP_NAME, JSON.stringify(reschedules));
    }
    return false;
  }

  /**
   * Returns true if the scheduler didn't run in the last 5 minutes
   */
  function isStalled() {
    const lastRun = _getDocPropService().getProperty(LAST_RUN_PROP_NAME) || null;
    return !lastRun || lastRun < (new Date()).getTime() - 1000*60*5; // 5 minutes in milliseconds
  }

  function _getRescheduled() {
    const reprops = _getDocPropService().getProperty(RESCHEDULES_PROP_NAME);
    return reprops ? JSON.parse(reprops) : {};
  }

  /**
   * Updates the last run timestamp
   */
  function _updateLastRun() {
    return _getDocPropService().setProperty(LAST_RUN_PROP_NAME, (new Date()).getTime());
  }

  function _getDocPropService() {
    return PropertiesService.getDocumentProperties();
  }
}/**
 * Cache wrapper.
 */
function BinCache() {
  const CURRENT_CACHE_KEYS_KEY = "current_cache_keys"; // In where current cached keys are stored

  return {
    read,
    write,
    clean
  };
  

  /**
  * Reads data from cache optionally applying a custom cache validation function.
  */
  function read(key, validate) {
    const CACHE_KEY = _craftKey(key);
    const cache = _getService();
    const data = cache.get(CACHE_KEY);
    const unzip_data = data ? Utilities.unzip(Utilities.newBlob(Utilities.base64Decode(data), "application/zip")) : null;
    const parsed = unzip_data ? JSON.parse(unzip_data[0].getAs("application/octet-stream").getDataAsString()) : [];

    // Apply cache validation if custom function was given
    return typeof validate === "function" ? (validate(parsed) ? parsed : []) : parsed;
  }

  /**
  * Writes data into cache.
  */
  function write(key, data, ttl) {
    const CACHE_KEY = _craftKey(key);
    const cache = _getService();
    const blob_data = Utilities.zip([Utilities.newBlob(JSON.stringify(data), "application/octet-stream")]);
    cache.put(CACHE_KEY, Utilities.base64Encode(blob_data.getBytes()), ttl);
    _pushCacheKeys(cache, key, ttl);
    return cache;
  }

  /**
   * Clean the entire cache!
   */
  function clean() {
    const cache = _getService();
    const keys = Object.keys(_currentCacheKeys(cache)).map(_craftKey);
    Logger.log("Removing current cache keys: "+JSON.stringify(keys));
    cache.removeAll(keys);
    cache.remove(CURRENT_CACHE_KEYS_KEY);
    return cache;
  }

  function _craftKey(key) {
    return "BIN_CACHE{"+key+"}";
  }

  function _pushCacheKeys(cache, key, ttl) {
    const now = (new Date()).getTime(); // Milliseconds
    const keys = _currentCacheKeys(cache);

    if (DEBUG) {
      Logger.log("PUSHING NEW CACHE KEY: "+key);
      Logger.log("CURRENT CACHE KEYS: "+JSON.stringify(keys));
    }

    // Clean expired keys and add the new one
    const newKeys = Object.keys(keys).reduce(function(acc, aKey) {
      const ts = keys[aKey];
      if (now < ts) { // This key is still alive!
        acc[aKey] = ts; // Keep it with its original timestamp
      }
      return acc;
    }, {[key]: now + ttl * 1000}); // Set current key expiration in milliseconds

    return cache.put(CURRENT_CACHE_KEYS_KEY, JSON.stringify(newKeys));
  }

  function _currentCacheKeys(cache) {
    const data = cache.get(CURRENT_CACHE_KEYS_KEY);
    return data ? JSON.parse(data) : {};
  }

  function _getService() {
    return CacheService.getUserCache();
  }
}/**
 * Initialization on document open.
 */
function onOpen(event) {
  if (DEBUG) {
    Logger.log("EVENT: "+JSON.stringify(event));
  }
  const setup = BinSetup();
  BinMenu(SpreadsheetApp.getUi()); // Add items to main menu
  if (setup.isReady()) { // Add-on is ready!
    setup.init();
    BinUtils().toast("Hi there! I'm installed and working at this spreadsheet. Enjoy!  =]");
  } else { // Add-on is not ready! It might be due to missing authorization or permissions removal (BinScheduler is stalled or never run)
    Logger.log("The add-on is NOT authorized!");
    BinUtils().toast("The add-on is NOT authorized! Click 'Authorize add-on!' button on 'Binance' menu TWICE.", "", 600);
  }

  Logger.log("Welcome to 'Binance to Google Sheets' by Diego Manuel, enjoy!  =]");
}

/**
 * Initialization on add-on install.
 * This would never happen since the add-on isn't installed from marketplace.. but whatever..!
 */
function onInstall(event) {
  Logger.log("Binance to Google Sheets was installed!");
  onOpen(event);
}

/**
 * Script setup functions wrapper.
 */
function BinSetup() {
  const API_KEY_NAME = "BIN_API_KEY";
  const API_SECRET_NAME = "BIN_API_SECRET";
  const WALLETS_DISABLED_NAME = "BIN_WALLETS_DISABLED";
  const SUB_ACCOUNTS_NAME = "BIN_SUB_ACCOUNTS";
  const user_props = PropertiesService.getUserProperties();

  return {
    init,
    authorize,
    isReady,
    areAPIKeysConfigured,
    getAPIKey,
    setAPIKey,
    getAPISecret,
    setAPISecret,
    configAPIKeys,
    clearAPIKeys,
    toggleWalletDisabled,
    getDisabledWallets,
    addSubAccount,
    removeSubAccount,
    getSubAccounts
  };

  /**
   * Runs initialization tasks
   */
  function init() {
    BinScheduler().init(); // Mark the scheduler as initialized
    _configTriggers(); // Create triggers to automatically keep the formulas updated
  }

  /**
   * Sadly, we can't detect from within the code when the user authorizes the add-on
   * since there is NO `onAuthorize` trigger or something like that (basically a mechanism to let the code know when it gets authorized).
   * The `onInstall` trigger only works when the add-on is installed from the Google Marketplace, but
   * this is not our case (and besides, marketplace add-ons can't have triggers smaller than 1 hour.. so totally discarded).
   * So, this is an "ugly workaround" to try to help the user to get things working in the initial setup!
   */
  function authorize() {
    init();
    Logger.log("The add-on is authorized, enjoy!");
    BinUtils().toast("The add-on is authorized and running, enjoy!", "Ready to rock", 10);
    BinUtils().refreshMenu(); // Refresh add-on's main menu items
  }

  /**
   * Returns true if the add-on is setup and ready to rock!
   */
  function isReady() {
    const isStalled = BinScheduler().isStalled();
    const authFull = ScriptApp.getAuthorizationInfo(ScriptApp.AuthMode.FULL).getAuthorizationStatus();
    const authLimited = ScriptApp.getAuthorizationInfo(ScriptApp.AuthMode.LIMITED).getAuthorizationStatus();
    return !isStalled &&
      (authFull === ScriptApp.AuthorizationStatus.NOT_REQUIRED || authLimited === ScriptApp.AuthorizationStatus.NOT_REQUIRED);
  }

  /**
   * Returns true is API keys are configured
   */
  function areAPIKeysConfigured() {
    return getAPIKey() && getAPISecret();
  }

  /**
   * Returns the Binance API Key
   */
  function getAPIKey() {
    return user_props.getProperty(API_KEY_NAME) || "";
  }
  /**
   * Returns the Binance API Key
   */
  function setAPIKey(value) {
    return user_props.setProperty(API_KEY_NAME, value);
  }

  /**
   * Returns the Binance API Secret
   */
  function getAPISecret() {
    return user_props.getProperty(API_SECRET_NAME) || "";
  }
  /**
   * Sets the Binance API Secret
   */
  function setAPISecret(value) {
    return user_props.setProperty(API_SECRET_NAME, value);
  }

  /**
   * API keys configuration.
   */
  function configAPIKeys(ui) {
    let text = "";
    let changed = false;
    let result = false;
    let user_input = null;

    // API KEY
    if (getAPIKey()) {
      text = "✅ Your Binance API Key is already set!\n\nYou can let it blank to keep the current one\nor re-enter a new one below to overwrite it:";
    } else {
      text = "Please enter your Binance API Key below:";
    }
    result = ui.prompt("Set Binance API Key", text, ui.ButtonSet.OK_CANCEL);
    user_input = result.getResponseText().replace(/\s+/g, '');
    if (result.getSelectedButton() != ui.Button.OK) {
      return false; // Cancel setup
    }
    if (user_input) {
      setAPIKey(user_input);
      changed = true;
      ui.alert("Binance API Key saved",
               "Your Binance API Key was successfully saved!",
               ui.ButtonSet.OK);
    }
  
    // API SECRET KEY
    if (getAPISecret()) {
      text = "✅ Your Binance API Secret Key is already set!\n\nYou can let it blank to keep the current one\nor re-enter a new one below to overwrite it:";
    } else {
      text = "Please enter your Binance API Secret Key below:";  
    }
    result = ui.prompt("Set Binance API Secret Key", text, ui.ButtonSet.OK_CANCEL);
    user_input = result.getResponseText().replace(/\s+/g, '');
    if (result.getSelectedButton() != ui.Button.OK) {
      return false; // Cancel setup
    }
    if (user_input) {
      setAPISecret(user_input);
      changed = true;
      ui.alert("Binance API Secret Key saved",
               "Your Binance API Secret Key was successfully saved!",
               ui.ButtonSet.OK);
    }

    const api_key = getAPIKey();
    const api_secret = getAPISecret();
    if (changed && api_key && api_secret) {
      return _refreshUI(); // Force UI refresh!
    }

    if (!api_key) {
      ui.alert("Binance API Key is not set!",
               "You just need a Binance API Key if you want to use private operations.\n\n"+
               "It's NOT needed to use public operations like current market prices and 24hr stats!",
               ui.ButtonSet.OK);
    }
    if (!api_secret) {
      ui.alert("Binance API Secret Key is not set!",
               "You just need a Binance API Secret Key if you to use private operations.\n\n"+
               "It's NOT needed to use public operations like current market prices and 24hr stats!",
               ui.ButtonSet.OK);
    }
  }

  /**
   * Clears configured API keys
   */
  function clearAPIKeys(ui) {
    const text = "Are you sure you want to remove your configured Binance API Keys?\n\nYou can always re-configure'em again later if you proceed here.";
    const result = ui.alert("Clear Binance API Keys", text, ui.ButtonSet.OK_CANCEL);
    if (result !== ui.Button.OK) {
      return false; // Cancel
    }

    user_props.deleteProperty(API_KEY_NAME);
    user_props.deleteProperty(API_SECRET_NAME);
    Logger.log("[clearAPIKeys] Binance API Keys were cleared!");
    BinUtils().toast("Binance API Keys were cleared! You can always re-configure'em again from 'Binance' main menu.", "", 30);
    _refreshUI(); // Force UI refresh!
  }

  /**
   * Toggles the enabled/disabled status for a given wallet type
   */
  function toggleWalletDisabled(type, ui, desc) {
    const wallets = getDisabledWallets();
    const disabled = !!wallets[type];
    const description = (desc || type).toUpperCase();
    const title = (disabled?"En":"Dis")+"able wallet: "+description;
    const text = "You will "+(disabled?"EN":"DIS")+"ABLE the "+description+" wallet!\nProceed?";
    const result = ui.alert(title, text, ui.ButtonSet.OK_CANCEL);
    if (result !== ui.Button.OK) {
      return false; // Cancel
    }

    Logger.log("[BinSetup] "+(disabled?"En":"Dis")+"abling wallet: "+description);
    wallets[type] = !disabled;
    user_props.setProperty(WALLETS_DISABLED_NAME, JSON.stringify(wallets));
    const bu = BinUtils();
    bu.refreshMenu();
    bu.toast("The "+description+" wallet was "+(disabled?"EN":"DIS")+"ABLED!\nPlease wait while assets are refreshed..", "", 30);

    Logger.log("[BinSetup] Clearing wallet assets and refreshing formulas..");
    const bw = BinWallet();
    bw.clearAssets(type);
    bw.refreshAssets();
    bu.forceRefreshSheetFormulas(BinDoAccountInfo().tag());

    return !disabled;
  }

  /**
   * Gets the current disabled wallets
   */
  function getDisabledWallets() {
    const data = user_props.getProperty(WALLETS_DISABLED_NAME);
    const parsed = data ? JSON.parse(data) : {};
    if (parsed["futures"] === undefined) { // Disabled FUTURES USD-M wallet by default
      parsed["futures"] = true;
    }
    if (parsed["delivery"] === undefined) { // Disabled FUTURES COIN-M wallet by default
      parsed["delivery"] = true;
    }
    return parsed;
  }

  /**
   * Adds a sub-account
   */
  function addSubAccount(ui) {
    const available = BinDoAccountInfo().listSubAccounts().map((subacc) => subacc.email);
    if (!available.length) {
      ui.alert("No Sub-Accounts Available",
               "You don't have any sub-account yet!\nYou must create one at Binance first.",
               ui.ButtonSet.OK);
      return false; // Cancel
    }
    Logger.log("[BinSetup] Available sub-accounts detected: "+available.length+"\n"+JSON.stringify(available));

    const subaccs = getSubAccounts();
    const text = "Please enter the sub-account email that you want to add:\n"+
      "\nAvailable sub-accounts detected: "+available.length+"\n"+available.join("\n")+"\n"+
      "\nCurrently added sub-accounts:\n"+(Object.keys(subaccs).join("\n")||"- none -");
    const result = ui.prompt("Add Sub-Account", text, ui.ButtonSet.OK_CANCEL);
    if (result.getSelectedButton() != ui.Button.OK) {
      return false; // Cancel
    }
    const email = result.getResponseText().replace(/\s+/g, '');
    if (!email) {
      return false; // Cancel
    }
    if (available.indexOf(email) === -1) {
      ui.alert("Sub-Account Not Found",
               "ERROR: The email '"+email+"' is not part of your available sub-accounts emails!\n\nAvailable sub-account emails:\n"+available.join("\n"),
               ui.ButtonSet.OK);
      return false; // Cancel
    }

    Logger.log("[BinSetup] Adding sub-account: "+email);
    subaccs[email] = email; // @TODO Add some useful data here?
    user_props.setProperty(SUB_ACCOUNTS_NAME, JSON.stringify(subaccs));
    BinDoAccountInfo().schedule(); // Update assets in the next "1m" trigger run
    BinUtils().refreshMenu();
    Logger.log("[BinSetup] Currently added sub-accounts:\n"+JSON.stringify(Object.keys(subaccs)));
    ui.alert("Sub-Account Added",
             "Email '"+email+"' was successfully added as sub-account!\n\nAccount assets will be refreshed in the next minute..",
             ui.ButtonSet.OK);
    return email;
  }

  /**
   * Removes a sub-account
   */
  function removeSubAccount(ui) {
    const subaccs = getSubAccounts();
    const text = "Please enter the sub-account email that you want to remove:\n"+
      "\nCurrently added sub-accounts:\n"+(Object.keys(subaccs).join("\n")||"- none -");
    const result = ui.prompt("Remove Sub-Account", text, ui.ButtonSet.OK_CANCEL);
    if (result.getSelectedButton() != ui.Button.OK) {
      return false; // Cancel
    }
    const email = result.getResponseText().replace(/\s+/g, '');
    if (!email) {
      return false; // Cancel
    }
    if (!subaccs[email]) {
      ui.alert("Remove Sub-Account",
               "ERROR: The email '"+email+"' doesn't belong to any added sub-account!",
               ui.ButtonSet.OK);
      return false; // Cancel
    }

    Logger.log("[BinSetup] Removing sub-account: "+email);
    delete subaccs[email];
    user_props.setProperty(SUB_ACCOUNTS_NAME, JSON.stringify(subaccs));
    BinDoAccountInfo().schedule(); // Update assets in the next "1m" trigger run
    BinUtils().refreshMenu();
    Logger.log("[BinSetup] Currently added sub-accounts:\n"+JSON.stringify(Object.keys(subaccs)));
    ui.alert("Sub-Account Removed",
             "Email '"+email+"' was successfully removed as sub-account!\n\nAccount assets will be refreshed in the next minute..",
             ui.ButtonSet.OK);
    return email;
  }

  /**
   * Gets the current added sub-accounts
   */
  function getSubAccounts() {
    const data = user_props.getProperty(SUB_ACCOUNTS_NAME);
    return data ? JSON.parse(data) : {};
  }

  /**
   * Configs required triggers to automatically have the data updated.
   */
  function _configTriggers() {
    // Time-based triggers config
    const triggers = {
      "doRefresh1m": [1, false],
      "doRefresh5m": [5, false],
      "doRefresh10m": [10, false],
      "doRefresh15m": [15, false],
      "doRefresh30m": [30, false],
      "doRefresh1h": [60, false],
      "doTablesPoll": [10, false] // This one will have its own trigger fixed at 10m
    };

    // First, check which triggers were already created
    ScriptApp.getProjectTriggers().map(function(trigger) {
      if (triggers[trigger.getHandlerFunction()]) { // This trigger already exists and belongs to this add-on
        if (DEBUG) { // Remove triggers while debugging..!
          ScriptApp.deleteTrigger(trigger);
        }
        triggers[trigger.getHandlerFunction()][1] = true; // Mark it as created
      }
    });

    if (DEBUG) {
      return Logger.log("[configTriggers] Removed add-on triggers and skipping creation while debugging..!");
    }

    try { // Create missing triggers (if any)
      return Object.keys(triggers).map(function(func) {
        const [triggerMinutes, triggerCreated] = triggers[func];
        if (triggerCreated) { // This trigger was marked as already created!
          Logger.log("[configTriggers] Trigger already setup every "+triggerMinutes+"m: "+func);
          return false; // Skip
        }
        Logger.log("[configTriggers] Creating new trigger every "+triggerMinutes+"m: "+func);
        return ScriptApp.newTrigger(func)
          .timeBased()
          [triggerMinutes < 60 ? "everyMinutes" : "everyHours"](triggerMinutes < 60 ? triggerMinutes : Math.floor(triggerMinutes / 60))
          .create();
      });
    } catch (err) {
      if (err.message.match(/trigger must be at least one hour/i)) {
        // This can only happen if it was installed as an add-on!
        // This is discouraged and the user should instead follow the setup steps in README.md
        // to properly setup the fully-working code with all the needed permissions.
        console.error("[configTriggers] Can't create <1h triggers!");
        BinUtils().toast("Couldn't create triggers to keep data updated! Follow the setup steps in README.md to have it working properly.", "", 30);
      }
      throw(err);
    }
  }

  function _refreshUI() {
    BinUtils().refreshMenu(); // Refresh main menu items
    return BinUtils().forceRefreshSheetFormulas(); // Force refresh all formulas!
  }
}

/**
 * These ones have to live here in the outside world
 * because of how `ScriptApp.newTrigger` works.
 */
function doRefresh1m(event) {
  _callScheduler(event, "1m");
}
function doRefresh5m(event) {
  _callScheduler(event, "5m");
}
function doRefresh10m(event) {
  _callScheduler(event, "10m");
}
function doRefresh15m(event) {
  _callScheduler(event, "15m");
}
function doRefresh30m(event) {
  _callScheduler(event, "30m");
}
function doRefresh1h(event) {
  _callScheduler(event, "60m");
}

function doTablesPoll(event) {
  if (DEBUG) {
    Logger.log("EVENT: "+JSON.stringify(event));
  }
  if (BinSetup().areAPIKeysConfigured()) {
    BinDoOrdersTable().execute();
  }
}

function _callScheduler(event, every) {
  if (DEBUG) {
    Logger.log("EVENT: "+JSON.stringify(event));
  }
  BinScheduler()["run"+every]();
}/**
 * Adds menu items under "Binance" at main menu.
 */
function BinMenu(ui) {
  /**
   * Adds the menu items to spreadsheet's main menu
   */
  function addMenuItems(menu) {
    const is_ready = BinSetup().isReady();

    if (!is_ready) { // Add-on is not ready (unauthorized or BinScheduler is stalled or never run)
      menu.addItem("Authorize add-on!", "authorizeMe");
    } else {
      menu.addItem("Refresh", "forceRefreshFormulas");
    }
    menu.addSeparator()
        .addItem("Show API Last Update", "showAPILastUpdate")
        .addSeparator();
    if (is_ready) { // Add-on is authorized and running fine!
      if (BinSetup().areAPIKeysConfigured()) {
        menu.addSubMenu(addWalletsMenu())
            .addSubMenu(addSubAccountsMenu())
            .addSeparator()
            .addItem("Show API Keys", "showAPIKeys")
            .addSubMenu(ui.createMenu("Setup API Keys")
              .addItem("Re-configure API Keys", "showAPIKeysSetup")
              .addItem("Clear API Keys", "showAPIKeysClear"));
      } else {
        menu.addItem("Setup API Keys", "showAPIKeysSetup");
      }
      menu.addSubMenu(ui.createMenu("Update Intervals")
            .addSubMenu(addTriggerIntervalItems("Prices", "Prices", BinDoCurrentPrices()))
            .addSubMenu(addTriggerIntervalItems("Historical Data", "HistoricalData", BinDoHistoricalData()))
            .addSubMenu(addTriggerIntervalItems("24h Stats", "24hStats", BinDo24hStats()))
            .addSubMenu(addTriggerIntervalItems("Account Info", "AccountInfo", BinDoAccountInfo()))
            .addSubMenu(addTriggerIntervalItems("Open Orders", "OrdersOpen", BinDoOrdersOpen()))
            .addSubMenu(addTriggerIntervalItems("Done Orders", "OrdersDone", BinDoOrdersDone()))
            .addSeparator()
            .addItem("Reset to Defaults", "resetTriggersIntervalConfig"));
      menu.addSeparator()
          .addItem("Credits", "showCredits")
          .addItem("Donate  =]", "showDonate")
          .addSeparator()
          .addItem("Version: "+VERSION, "showVersion");
    }
    
    menu.addToUi(); // Add menu items to the spreadsheet main menu
  }

  function addWalletsMenu() {
    const disabled = BinSetup().getDisabledWallets();
    const walletEnabled = (type) => disabled[type] ? "Enable" : "Disable";
    return ui.createMenu("Wallets")
      .addItem(walletEnabled("cross")+" CROSS Margin Wallet", "toggleWalletCross")
      .addItem(walletEnabled("isolated")+" ISOLATED Margin Wallet", "toggleWalletIsolated")
      .addItem(walletEnabled("futures")+" FUTURES USD-M Wallet", "toggleWalletFutures")
      .addItem(walletEnabled("delivery")+" FUTURES COIN-M Wallet", "toggleWalletDelivery");
  }

  function addSubAccountsMenu() {
    const subaccs = BinSetup().getSubAccounts();
    const menu = ui.createMenu("Sub-Accounts")
      //.addItem("Show List", "showSubAccountsList") // @TODO Add sub-account list
      .addItem("Add Sub-Account", "showSubAccountsAdd");

    if (Object.keys(subaccs).length) {
      menu.addSeparator();
      Object.keys(subaccs).forEach(function(email) {
        menu.addItem(email, "showSubAccountsRemove");
      });
      menu.addSeparator()
        .addItem("Remove Sub-Accounts", "showSubAccountsRemove");
    }

    return menu;
  }

  /**
   * Helper to define each trigger's interval options
   */
  function addTriggerIntervalItems(menuText, func, module) {
    const funcName = "setIntervalFor"+func;
    const intervalSelected = (interval) => module.period() === interval ? "[X] " : "";
    return ui.createMenu(menuText+" ("+module.period()+")")
      .addItem(intervalSelected("1m")+"1 minute", funcName+"1m")
      .addItem(intervalSelected("5m")+"5 minutes", funcName+"5m")
      .addItem(intervalSelected("10m")+"10 minutes", funcName+"10m")
      .addItem(intervalSelected("15m")+"15 minutes", funcName+"15m")
      .addItem(intervalSelected("30m")+"30 minutes", funcName+"30m")
      .addItem(intervalSelected("60m")+"1 hour", funcName+"60m")
      .addSeparator()
      .addItem(intervalSelected("off")+"OFF!", funcName+"Off");
  }

  // Add the menu to the UI
  addMenuItems(ui.createMenu("Binance"));
  addMenuItems(ui.createAddonMenu());
}

/**
 * Forces all BINANCE() formulas recalculation on the current spreadsheet.
 * Cleans the cache first to ensure getting fresh data from Binance API!
 */
function forceRefreshFormulas() {
  const utils = BinUtils();
  utils.toast("Refreshing data, be patient..!", "", 5);
  BinCache().clean(); // Clean cache!
  utils.forceRefreshSheetFormulas(); // Refresh'em all!
}

/**
 * Triggers the modal to enable/authorize the add-on.
 * If this function gets executed, it means that the user has authorized the add-on!
 */
function authorizeMe() {
  BinSetup().authorize();
}

/**
 * Displays a modal with the datetime of the last Binance API call.
 */
function showAPILastUpdate() {
  const ui = SpreadsheetApp.getUi();
  const last_update = BinDoLastUpdate().run();
  const formatted = last_update+"" || "- never called yet -";
  ui.alert("Binance API last call", formatted, ui.ButtonSet.OK);
}

/**
 * Displays a confirmation to enable/disable CROSS wallet
 */
function toggleWalletCross() {
  BinSetup().toggleWalletDisabled("cross", SpreadsheetApp.getUi());
}

/**
 * Displays a confirmation to enable/disable ISOLATED wallet
 */
function toggleWalletIsolated() {
  BinSetup().toggleWalletDisabled("isolated", SpreadsheetApp.getUi());
}

/**
 * Displays a confirmation to enable/disable FUTURES USD-M wallet
 */
function toggleWalletFutures() {
  BinSetup().toggleWalletDisabled("futures", SpreadsheetApp.getUi(), "FUTURES USD-M");
}

/**
 * Displays a confirmation to enable/disable FUTURES COIN-M wallet
 */
function toggleWalletDelivery() {
  BinSetup().toggleWalletDisabled("delivery", SpreadsheetApp.getUi(), "FUTURES COIN-M");
}

/**
 * Displays a modal to add a sub-account.
 */
function showSubAccountsAdd() {
  BinSetup().addSubAccount(SpreadsheetApp.getUi());
}

/**
 * Displays a modal to remove a sub-account.
 */
function showSubAccountsRemove() {
  BinSetup().removeSubAccount(SpreadsheetApp.getUi());
}

/**
 * Displays a modal with currently configured API keys.
 */
function showAPIKeys() {
  const ui = SpreadsheetApp.getUi();
  ui.alert("Binance API Keys",
           "API Key:\n"+
           (BinSetup().getAPIKey() || "- not set -")+"\n"+
           "\n"+
           "API Secret Key:\n"+
           (BinUtils().obscureSecret(BinSetup().getAPISecret()) || "- not set -")
           ,ui.ButtonSet.OK);
}

/**
 * Displays a modal to setup API keys.
 */
function showAPIKeysSetup() {
  BinSetup().configAPIKeys(SpreadsheetApp.getUi());
}

/**
 * Displays a modal to setup API keys.
 */
function showAPIKeysClear() {
  BinSetup().clearAPIKeys(SpreadsheetApp.getUi());
}

/**
 * Displays a modal with the current running version info
 */
function showVersion() {
  const ui = SpreadsheetApp.getUi();
  const title = "Binance to Google Sheets - "+VERSION;
  const body = "Diego Manuel - diegomanuel@gmail.com - Argentina\n"+
               REPO_URL+"\n"+
               "\n"+
               "\n"+
               "You are running version: '"+VERSION+"'\n"+
               "Check the github repo for the latest updates.\n"+
               "\n"+
               "\n"+
               "\n"+
               "Keep enjoying!  =]";
  ui.alert(title, body, ui.ButtonSet.OK);
  Logger.log("[Version] "+title);
}

/**
 * Displays a modal with the developer's credits!  =]
 */
function showCredits() {
  const ui = SpreadsheetApp.getUi();
  const title = "Credits - Binance to Google Sheets - "+VERSION;
  const body = "Diego Manuel - diegomanuel@gmail.com - Argentina\n"+
               REPO_URL+"\n"+
               "\n"+
               "\n"+
               "Diego says: Hello there folks!\n"+
               "Hope you enjoy this handy tool as it currently is for myself.  =]\n"+
               "\n"+
               "Some background: Why this tool had ever to come alive?!\n"+
               "I needed a way to have Binance data directly available at my Google Spreadsheet.\n"+
               "First, I've looked for several existing solutions, but none provided me the freedom, confidence and privacy that I want for this kind of delicate stuff.\n"+
               "It's a requirement for me that requests to Binance go directly from my spreadsheet to its API without any intermediary service in between.\n"+
               "So I decided to write my own code, all from scratch, with only my will and my javascript knownledge aboard..\n"+
               "..and I was so happy with the results that I simply decided to share it to the world!\n"+
               "\n"+
               "\n"+
               "I think and hope that many of you will find it as useful as it is for myself.\n"+
               "\n"+
               "Enjoy, cheers!";
  ui.alert(title, body, ui.ButtonSet.OK);
  Logger.log("[Credits] Diego Manuel - diegomanuel@gmail.com - "+REPO_URL);
}

/**
 * Displays a modal with the donation options  =]
 */
function showDonate() {
  const ui = SpreadsheetApp.getUi();
  const title = "Donate - Buy me a beer!  =]";
  const body = "Thank you for using Binance to Google Sheets add-on!\n"+
               "I really hope you enjoyed and loved it as much as I love to use it everyday.\n"+
               "\n"+
               "If your love is strong enough, feel free to share it with me!  =D\n"+
               "I will much appreciate any contribution and support to keep working on it.\n"+
               "I have several ideas for new features, so much more could come!\n"+
               "\n"+
               "\n"+
               "You can send any token through the Binance Smart Chain (BSC/BEP20) to:\n"+
               "0x1d047bc3e46ce0351fd0c44fc2a2029512e87a97\n"+
               "\n"+
               "But you can also use:\n"+
               "[BTC] BTC: 1FsN54WNibhhPhRt4vnAPRGgzaVeeFvEnM\n"+
               "[BTC] SegWit: bc1qanxn2ycp9em50hj5p7en6wxe962zj4umqvs7q9\n"+
               "[ETH] ERC20: 0x1d047bc3e46ce0351fd0c44fc2a2029512e87a97\n"+
               "[LTC] LTC: LZ8URuChzyuuy272isMCrts7R7UKtwnj6a\n"+
               "\n"+
               "------------------------------------------------\n"+
               "Don't you have a Binance account yet?\n"+
               "Register using the referal link below and get a 10% discount on fees for all your trades!\n"+
               "https://www.binance.com/en/register?ref=SM93PRAV\n"+
               "------------------------------------------------\n"+
               "\n"+
               "This software was published and released under the GPL-3.0 License.\n"+
               "\n"+
               "Use it wisely, happy trading!\n"+
               "Diego.";
  ui.alert(title, body, ui.ButtonSet.OK);
  Logger.log("[Donate] Buy me a beer!  =]");
}

/**
 * Below here, all the functions to configure trigger intervals
 */
function resetTriggersIntervalConfig() {
  const ui = SpreadsheetApp.getUi();
  const text = "Proceed to reset your intervals configuration for triggers to default values?";
  const result = ui.alert("Confirm reset to defaults", text, ui.ButtonSet.OK_CANCEL);
  if (result == ui.Button.OK) {
    BinScheduler().cleanSchedules();
    BinUtils().toast("Intervals configuration for triggers were reset to its defaults!");
    BinUtils().refreshMenu(); // Refresh main menu items
  }
}

function _setIntervalFor(operation, interval) {
  BinScheduler().setSchedule(operation, interval);
  BinUtils().toast("Configured schedule for ["+operation+"] every ["+interval+"]");
  BinUtils().refreshMenu(); // Refresh main menu items
}

// PRICES
function _setIntervalForPrices(interval) {
  _setIntervalFor(BinDoCurrentPrices().tag(), interval);
}
function setIntervalForPricesOff() {
  _setIntervalForPrices("off");
}
function setIntervalForPrices1m() {
  _setIntervalForPrices("1m");
}
function setIntervalForPrices5m() {
  _setIntervalForPrices("5m");
}
function setIntervalForPrices10m() {
  _setIntervalForPrices("10m");
}
function setIntervalForPrices15m() {
  _setIntervalForPrices("15m");
}
function setIntervalForPrices30m() {
  _setIntervalForPrices("30m");
}
function setIntervalForPrices60m() {
  _setIntervalForPrices("60m");
}

// HISTORICAL DATA
function _setIntervalForHistoricalData(interval) {
  _setIntervalFor(BinDoHistoricalData().tag(), interval);
}
function setIntervalForHistoricalDataOff() {
  _setIntervalForHistoricalData("off");
}
function setIntervalForHistoricalData1m() {
  _setIntervalForHistoricalData("1m");
}
function setIntervalForHistoricalData5m() {
  _setIntervalForHistoricalData("5m");
}
function setIntervalForHistoricalData10m() {
  _setIntervalForHistoricalData("10m");
}
function setIntervalForHistoricalData15m() {
  _setIntervalForHistoricalData("15m");
}
function setIntervalForHistoricalData30m() {
  _setIntervalForHistoricalData("30m");
}
function setIntervalForHistoricalData60m() {
  _setIntervalForHistoricalData("60m");
}

// 24H STATS
function _setIntervalFor24hStats(interval) {
  _setIntervalFor(BinDo24hStats().tag(), interval);
}
function setIntervalFor24hStatsOff() {
  _setIntervalFor24hStats("off");
}
function setIntervalFor24hStats1m() {
  _setIntervalFor24hStats("1m");
}
function setIntervalFor24hStats5m() {
  _setIntervalFor24hStats("5m");
}
function setIntervalFor24hStats10m() {
  _setIntervalFor24hStats("10m");
}
function setIntervalFor24hStats15m() {
  _setIntervalFor24hStats("15m");
}
function setIntervalFor24hStats30m() {
  _setIntervalFor24hStats("30m");
}
function setIntervalFor24hStats60m() {
  _setIntervalFor24hStats("60m");
}

// ACCOUNT INFO
function _setIntervalForAccountInfo(interval) {
  _setIntervalFor(BinDoAccountInfo().tag(), interval);
}
function setIntervalForAccountInfoOff() {
  _setIntervalForAccountInfo("off");
}
function setIntervalForAccountInfo1m() {
  _setIntervalForAccountInfo("1m");
}
function setIntervalForAccountInfo5m() {
  _setIntervalForAccountInfo("5m");
}
function setIntervalForAccountInfo10m() {
  _setIntervalForAccountInfo("10m");
}
function setIntervalForAccountInfo15m() {
  _setIntervalForAccountInfo("15m");
}
function setIntervalForAccountInfo30m() {
  _setIntervalForAccountInfo("30m");
}
function setIntervalForAccountInfo60m() {
  _setIntervalForAccountInfo("60m");
}

// OPEN ORDERS
function _setIntervalForOrdersOpen(interval) {
  _setIntervalFor(BinDoOrdersOpen().tag(), interval);
}
function setIntervalForOrdersOpenOff() {
  _setIntervalForOrdersOpen("off");
}
function setIntervalForOrdersOpen1m() {
  _setIntervalForOrdersOpen("1m");
}
function setIntervalForOrdersOpen5m() {
  _setIntervalForOrdersOpen("5m");
}
function setIntervalForOrdersOpen10m() {
  _setIntervalForOrdersOpen("10m");
}
function setIntervalForOrdersOpen15m() {
  _setIntervalForOrdersOpen("15m");
}
function setIntervalForOrdersOpen30m() {
  _setIntervalForOrdersOpen("30m");
}
function setIntervalForOrdersOpen60m() {
  _setIntervalForOrdersOpen("60m");
}

// DONE ORDERS
function _setIntervalForOrdersDone(interval) {
  _setIntervalFor(BinDoOrdersDone().tag(), interval);
}
function setIntervalForOrdersDoneOff() {
  _setIntervalForOrdersDone("off");
}
function setIntervalForOrdersDone1m() {
  _setIntervalForOrdersDone("1m");
}
function setIntervalForOrdersDone5m() {
  _setIntervalForOrdersDone("5m");
}
function setIntervalForOrdersDone10m() {
  _setIntervalForOrdersDone("10m");
}
function setIntervalForOrdersDone15m() {
  _setIntervalForOrdersDone("15m");
}
function setIntervalForOrdersDone30m() {
  _setIntervalForOrdersDone("30m");
}
function setIntervalForOrdersDone60m() {
  _setIntervalForOrdersDone("60m");
}
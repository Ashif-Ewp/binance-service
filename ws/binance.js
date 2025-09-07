const request = require("request");

/**
 * Binance REST API wrapper with enhanced error handling and logging.
 */
module.exports = class Binance {
  constructor() {
    this.baseUrl = "https://fapi.binance.com";
    this.requestCount = 0;
    this.lastRequestTime = 0;
  }

  /**
   * Current exchange trading rules and symbol information.
   * @returns {Promise} Response promise.
   */
  exchangeInfo() {
    return this.request("/fapi/v1/exchangeInfo");
  }

  /**
   * Kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.
   * @param {string} symbol - Trading symbol.
   * @param {string} interval - Klines interval.
   * @param {number} startTime - Start time in milliseconds.
   * @param {number} endTime - End time in milliseconds.
   * @param {number} limit - Klines limit.
   * @returns {Promise} Response promise.
   */
  klines(symbol, interval, startTime, endTime, limit) {
    // Enhanced parameter validation
    if (!symbol || typeof symbol !== "string") {
      const error = new Error("Invalid symbol parameter");
      error.code = "INVALID_SYMBOL";
      throw error;
    }

    if (!interval || typeof interval !== "string") {
      const error = new Error("Invalid interval parameter");
      error.code = "INVALID_INTERVAL";
      throw error;
    }

    if (isNaN(startTime) || startTime < 0) {
      const error = new Error("Invalid startTime parameter");
      error.code = "INVALID_START_TIME";
      throw error;
    }

    if (isNaN(endTime) || endTime < 0) {
      const error = new Error("Invalid endTime parameter");
      error.code = "INVALID_END_TIME";
      throw error;
    }

    if (isNaN(limit) || limit < 1 || limit > 1500) {
      const error = new Error("Invalid limit parameter (must be 1-1500)");
      error.code = "INVALID_LIMIT";
      throw error;
    }

    if (startTime >= endTime) {
      const error = new Error("startTime must be less than endTime");
      error.code = "INVALID_TIME_RANGE";
      throw error;
    }

    console.log(
      `[Binance API] Requesting klines: ${symbol} ${interval} ${new Date(
        startTime
      ).toISOString()} - ${new Date(endTime).toISOString()} (${limit})`
    );

    return this.request("/fapi/v1/klines", {
      qs: {
        symbol: symbol.toUpperCase(),
        interval,
        startTime,
        endTime,
        limit,
      },
    });
  }

  /**
   * Get 24hr ticker price change statistics
   * @param {string} symbol - Trading symbol (optional)
   * @returns {Promise} Response promise.
   */
  ticker24hr(symbol = null) {
    const params = {};
    if (symbol) {
      params.symbol = symbol.toUpperCase();
    }

    return this.request("/fapi/v1/ticker/24hr", {
      qs: params,
    });
  }

  /**
   * Get current price for a symbol
   * @param {string} symbol - Trading symbol (optional)
   * @returns {Promise} Response promise.
   */
  tickerPrice(symbol = null) {
    const params = {};
    if (symbol) {
      params.symbol = symbol.toUpperCase();
    }

    return this.request("/fapi/v1/ticker/price", {
      qs: params,
    });
  }

  /**
   * Common request method with enhanced error handling and rate limiting.
   * @param {string} path - API path.
   * @param {object} options - Request options.
   * @returns {Promise} Response promise.
   */
  request(path, options = {}) {
    return new Promise((resolve, reject) => {
      // Simple rate limiting
      const now = Date.now();
      if (now - this.lastRequestTime < 100) {
        // 100ms between requests
        setTimeout(() => {
          this.makeRequest(path, options, resolve, reject);
        }, 100 - (now - this.lastRequestTime));
      } else {
        this.makeRequest(path, options, resolve, reject);
      }
    });
  }

  /**
   * Make the actual HTTP request
   * @param {string} path - API path
   * @param {object} options - Request options
   * @param {function} resolve - Promise resolve function
   * @param {function} reject - Promise reject function
   */
  makeRequest(path, options, resolve, reject) {
    this.requestCount++;
    this.lastRequestTime = Date.now();

    const requestOptions = {
      timeout: 10000, // 10 second timeout
      headers: {
        "User-Agent": "TradingView-UDF-WebSocket/1.0",
        "Content-Type": "application/json",
      },
      ...options,
    };

    const startTime = Date.now();

    request(this.baseUrl + path, requestOptions, (err, res, body) => {
      const duration = Date.now() - startTime;

      if (err) {
        console.error(`[Binance API] Request failed after ${duration}ms:`, {
          path,
          error: err.message,
          code: err.code,
          requestCount: this.requestCount,
        });

        // Enhance error with additional context
        const enhancedError = new Error(
          `Binance API request failed: ${err.message}`
        );
        enhancedError.originalError = err;
        enhancedError.path = path;
        enhancedError.duration = duration;
        enhancedError.code = err.code || "NETWORK_ERROR";

        return reject(enhancedError);
      }

      if (!res) {
        const error = new Error("No response received from Binance API");
        error.code = "NO_RESPONSE";
        error.path = path;
        return reject(error);
      }

      // Log response details
      console.log(`[Binance API] Response received (${duration}ms):`, {
        path,
        statusCode: res.statusCode,
        contentLength: body ? body.length : 0,
        requestCount: this.requestCount,
      });

      if (!body) {
        const error = new Error("Empty response body from Binance API");
        error.code = "EMPTY_RESPONSE";
        error.statusCode = res.statusCode;
        error.path = path;
        return reject(error);
      }

      // Handle non-200 status codes
      if (res.statusCode !== 200) {
        console.error(`[Binance API] HTTP ${res.statusCode}:`, {
          path,
          statusCode: res.statusCode,
          body: body.substring(0, 500), // Log first 500 chars
        });

        const error = new Error(`HTTP ${res.statusCode}: ${res.statusMessage}`);
        error.statusCode = res.statusCode;
        error.path = path;
        error.body = body;

        return reject(error);
      }

      try {
        const json = JSON.parse(body);

        // Handle Binance API errors
        if (json.code && json.msg) {
          console.error(`[Binance API] API Error:`, {
            path,
            code: json.code,
            message: json.msg,
            requestCount: this.requestCount,
          });

          const error = new Error(json.msg);
          error.code = json.code;
          error.path = path;
          error.binanceError = true;

          return reject(error);
        }

        // Success
        console.log(`[Binance API] Success:`, {
          path,
          dataLength: Array.isArray(json) ? json.length : "object",
          duration,
          requestCount: this.requestCount,
        });

        return resolve(json);
      } catch (parseError) {
        console.error(`[Binance API] JSON parse error:`, {
          path,
          error: parseError.message,
          body: body.substring(0, 200), // Log first 200 chars for debugging
        });

        const error = new Error(
          `Failed to parse JSON response: ${parseError.message}`
        );
        error.originalError = parseError;
        error.path = path;
        error.body = body;
        error.code = "JSON_PARSE_ERROR";

        return reject(error);
      }
    });
  }

  /**
   * Get API statistics
   * @returns {object} Statistics object
   */
  getStats() {
    return {
      requestCount: this.requestCount,
      lastRequestTime: this.lastRequestTime,
      baseUrl: this.baseUrl,
    };
  }

  /**
   * Reset statistics
   */
  resetStats() {
    this.requestCount = 0;
    this.lastRequestTime = 0;
  }
};

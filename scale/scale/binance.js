// /home/ashif/chart_raiders/binance-service/scale/binance.js
const request = require("request");

module.exports = class Binance {
  exchangeInfo() {
    return this.request("/fapi/v1/exchangeInfo");
  }

  klines(symbol, interval, startTime, endTime, limit) {
    if (
      !symbol ||
      !interval ||
      isNaN(startTime) ||
      isNaN(endTime) ||
      isNaN(limit)
    ) {
      console.error("[Binance API] Invalid Kline Params:", {
        symbol,
        interval,
        startTime,
        endTime,
        limit,
      });
      throw new Error("Invalid parameters for Binance Klines API");
    }

    console.log({
      symbol,
      interval,
      startTime,
      endTime,
      limit,
    });

    return this.request("/fapi/v1/klines", {
      qs: { symbol, interval, startTime, endTime, limit },
    });
  }

  request(path, options) {
    return new Promise((resolve, reject) => {
      request("https://fapi.binance.com" + path, options, (err, res, body) => {
        if (res && res.headers["x-mbx-used-weight-1m"]) {
          console.log(
            `API Weight Used: ${res.headers["x-mbx-used-weight-1m"]}`
          );
        }
        if (res && res.statusCode === 429) {
          console.warn("Rate limit exceeded, retrying after 1s...");
          setTimeout(
            () => this.request(path, options).then(resolve).catch(reject),
            1000
          );
          return;
        }
        if (err) return reject(err);
        if (!body) return reject(new Error("No body"));
        try {
          const json = JSON.parse(body);
          if (json.code && json.msg) {
            const err = new Error(json.msg);
            err.code = json.code;
            return reject(err);
          }
          return resolve(json);
        } catch (err) {
          return reject(err);
        }
      });
    });
  }
};

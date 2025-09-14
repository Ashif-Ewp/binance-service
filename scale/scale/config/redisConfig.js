// redisConfig.js
const Redis = require("ioredis");

class RedisSingleton {
  constructor() {
    if (!RedisSingleton.instance) {
      RedisSingleton.instance = new Redis({
        host: "54.84.43.18" || "127.0.0.1",
        port: 16379,
        password: "my_master_password",
        retryStrategy(times) {
          return Math.min(times * 50, 2000);
        },
      });

      RedisSingleton.instance.on("connect", () => {
        console.log("✅ Redis server connected");
      });

      RedisSingleton.instance.on("error", (err) => {
        console.error("❌ Redis error:", err);
      });
    }
  }

  getInstance() {
    return RedisSingleton.instance;
  }
}

module.exports = new RedisSingleton();

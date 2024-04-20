function promisify(fn) {
  return (...args) => {
    return new Promise((resolve, reject) => {
      fn(...args, (err, data) => {
        if (err && err instanceof Error) {
          reject(err);
        } else if(err && Object.keys(err).length > 0) {
          reject(err);
        }
        else {
          resolve(data);
        }
      });
    });
  };
}

module.exports = promisify;
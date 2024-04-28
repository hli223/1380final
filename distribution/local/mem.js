const id = require('../util/id');
const mem = {
  tempMem: {},
};

mem.get = function(key, callback) {
  callback = callback || function() {};
  if (key === null) {
    callback(null, Object.keys(this.tempMem));
    return;
  }
  if (typeof key !== 'string') {
    key = key['key'];
  }
  if (this.tempMem.hasOwnProperty(key)) {
    callback(null, this.tempMem[key]);
    return;
  }
  callback(new Error(`Failed to get: key ${key} does not exist`));
};

mem.put = function(value, key, callback) {
  callback = callback || function() {};
  if (key === null) {
    key = id.getID(value);
  }
  console.log('calling local mem put function!');
    if (this.tempMem.hasOwnProperty(key)) {
        console.log(`key ${key} exists!`);
        if (!Array.isArray(this.tempMem[key])) {
            this.tempMem[key] = [this.tempMem[key]];
        }
        this.tempMem[key].push(value);
        callback(null, value);
        return;
    }
  this.tempMem[key] = value;
  callback(null, value);
};

mem.del = function(key, callback) {
  callback = callback || function() {};
  if (key in this.tempMem) {
    var deleted = this.tempMem[key];
    delete this.tempMem[key];
    callback(null, deleted);
    return;
  }
  callback(new Error('Failed to delete: key does not exist'), null);
};

module.exports = mem;
const id = require('../util/id');
const mem = {
  tempMem: {},
};

mem.get = function(keyGid, callback) {
  callback = callback || function() {};
  let key = keyGid.key;
  if (key === null) {
    console.log('calling local mem get function! on null', this.tempMem, Object.keys(this.tempMem));
    callback(null, Object.keys(this.tempMem));
    return;
  }
  if (this.tempMem.hasOwnProperty(key)) {
    console.log('local mem get success: ', key, this.tempMem[key]);
    callback(null, this.tempMem[key]);
    return;
  }
  callback(new Error(`Failed to get: key ${key} does not exist`));
};

mem.put = function(value, keyGid, callback) {
    const key = keyGid.key;
  callback = callback || function() {};
  if (key === null) {
    key = id.getID(value);
  }
  // console.log('calling local mem put function!', keyGid, value, key);
    if (this.tempMem.hasOwnProperty(key)) {
        console.log('key exists!', key);
        if (!Array.isArray(this.tempMem[key])) {
            this.tempMem[key] = [this.tempMem[key]];
        }
        if (Array.isArray(value)) {
            this.tempMem[key] = this.tempMem[key].concat(value);
        } else {
            this.tempMem[key].push(value);
        }
        // console.log('push to array!', this.tempMem[key]);
        callback(null, value);
        return;
    }
  this.tempMem[key] = value;
  callback(null, value);
};

mem.clear = function(callback) {
    this.tempMem = {};
    callback(null, null);
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
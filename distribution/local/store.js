//  ________________________________________
// / NOTE: You should use absolute paths to \
// | make sure they are agnostic to where   |
// | your code is running from! Use the     |
// \ `path` module for that purpose.        /
//  ----------------------------------------
//         \   ^__^
//          \  (oo)\_______
//             (__)\       )\/\
//                 ||----w |
//                 ||     ||
const id = require('../util/id');
const fs = require('fs');
const path = require('path');
const store = {};
const nid = id.getSID(global.nodeConfig);
const basePath = path.join(__dirname, 'storeFiles/'+nid);
const serialization = require('../util/serialization');


store.get = function(key, callback) {
  callback = callback || function() {};
  if (key === null) {
    // TODO: return a list that contains all filenames under storeFiles
    fs.readdir(basePath, (err, files) => {
      if (err) {
        callback(new Error('Failed to list files'));
        return;
      }
      callback(null, files);
    });
  } else {
    if (typeof key !== 'string') {
      key = key['key'];
    }
    const filePath = path.join(basePath, key);
    fs.readFile(filePath, (err, data) => {
      if (err) {
        callback(new Error('Failed to get: key does not exist'));
        return;
      }
      const value = serialization.deserialize(data.toString());
      callback(null, value);
    });
  }
};

store.put = function(value, key, callback) {
  if (!fs.existsSync(basePath)) {
    fs.mkdirSync(basePath, {recursive: true});
  }
  callback = callback || function() {};
  if (key === null) {
    key = id.getID(value);
  }
  const filePath = path.join(basePath, key);
  const serializedValue = serialization.serialize(value);
  fs.writeFile(filePath, serializedValue, (err) => {
    if (err) return callback(err);
    callback(null, value);
  });
};

store.del = function(key, callback) {
  callback = callback || function() {};
  const filePath = path.join(basePath, key);

  fs.readFile(filePath, (err, data) => {
    if (err) {
      callback(new Error('Failed to delete: key does not exist'));
      return;
    }
    const value = serialization.deserialize(data.toString());


    fs.unlink(filePath, (err) => {
      if (err) {
        callback(new Error('Failed to delete: key does not exist'));
        return;
      }
    });
    callback(null, value);
  });
};

module.exports = store;


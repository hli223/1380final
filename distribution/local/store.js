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
const basePath = path.join(__dirname, '../../store/'+nid);
const serialization = require('../util/serialization');
if (!fs.existsSync(basePath)) {
    fs.mkdirSync(basePath, {recursive: true});
}

store.gidKeys = {}

store.get = function(keyGid, callback) {
  callback = callback || function() {};
  if (keyGid.key === null) {
    // Return a list of keys under the specified gid
    callback(null, store.gidKeys[keyGid.gid]);
  } else {
    let key = keyGid.key;
    let gid = keyGid.gid;
    let keyId = id.getID(key);
    const filePath = path.join(basePath, gid + '-' + keyId);
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

store.put = function(value, keyGid, callback) {
//   if (!fs.existsSync(basePath)) {
//     fs.mkdirSync(basePath, {recursive: true});
//   }
  callback = callback || function() {};
  let key = keyGid.key;
  let gid = keyGid.gid;
  if (key === null) {
    key = id.getID(value);
  }
  let keyId = id.getID(key);
  const filePath = path.join(basePath, gid+'-'+keyId);
  const serializedValue = serialization.serialize(value);
  fs.writeFile(filePath, serializedValue, (err) => {
    if (err) return callback(err);
    if (!store.gidKeys[gid]) {
      store.gidKeys[gid] = [];
    }
    if (!store.gidKeys[gid].includes(key)) {
      store.gidKeys[gid].push(key);
    }
    callback(null, value);
  });
};

store.del = function(keyGid, callback) {
  callback = callback || function() {};
  const key = keyGid.key;
  const gid = keyGid.gid;
  const filePath = path.join(basePath, gid + '-' + key);
  fs.readFile(filePath, (err, data) => {
    if (err) {
      callback(new Error('Failed to readFile in delete, key: ' + key + ' err: ' + err));
      return;
    }
    const value = serialization.deserialize(data.toString());


    fs.unlink(filePath, (err) => {
      if (err) {
        callback(new Error('Failed to delete: key does not exist, key: ' + key + ' err: ' + err));
        return;
      }
    });
    callback(null, value);
  });
};

module.exports = store;


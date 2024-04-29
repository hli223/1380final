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
  if (!keyGid.key) {
    // Return a list of keys under the specified gid
    fs.readdir(basePath, (error, files) => {
      if (error) {
        callback(error);
      } else {
        let keys = [];
        files.forEach(file => {
          let gidKeyId = file.split('-');
          let gid = gidKeyId[0];
          if (gid !== keyGid.gid) {
            return;
          }
          let keyId = gidKeyId[gidKeyId.length - 1];
          keys.push(Buffer.from(keyId, 'base64').toString());
        });
        callback(null, keys);
      }
    });

  } else {
    let key = keyGid.key;
    let gid = keyGid.gid;
    let keyId = Buffer.from(key).toString("base64");
    const filePath = path.join(basePath, gid + '-' + keyId);
    fs.readFile(filePath, (err, data) => {
      if (err) {
        callback(new Error(`Failed to retrieve the value: The key '${key}' does not exist in the store.`));
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
  let keyId = Buffer.from(key).toString("base64");
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
  let keyId = Buffer.from(key).toString("base64");
  const filePath = path.join(basePath, gid+'-'+keyId); 
  fs.readFile(filePath, (err, data) => {
    if (err) {
      callback(new Error(`Failed to delete: key does not exist, key: ${key} err: ${err}`));
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


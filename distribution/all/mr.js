const mr = function (config) {
  let context = {};
  context.gid = config.gid || 'all';

  return {
    exec: (configuration, callback) => {
      /* Change this with your own exciting Map Reduce code! */
      // extract the map and reduce functions from the configuration
      const map = configuration.map;
      const reduce = configuration.reduce;
      const keys = configuration.keys;
      const compact = configuration.compact || ((x) => {
        return x;
      });
      console.log('compact are ', compact);

      // create a notification service and will use this service
      // to notify the nodes in the group to run the map and reduce functions
      // generate a random integer number
      let mrId = Math.floor(Math.random() * 1000);
      mrId = 'mr-' + mrId.toString();

      // wrape up the map function
      let mapWrapper = function (keys, map, gid, compact, cb) {
        console.log('map input keys are: ', keys);
        let newKeys = [];
        let count = 0;
        // let res = [];
        for (let i = 0; i < keys.length; i++) {
          global.distribution.local.store.get({ gid: gid, key: keys[i] },
            (e, v) => {
              // if the key exists in the current node
              if (e === null) {
                console.log('find: ', keys[i], v);
                let out = null;
                if (map.constructor.name === 'AsyncFunction') {
                  map(keys[i], v).then((result) => {
                    out = result;
                    console.log('promise result is: ', out);
                    if (Array.isArray(out)) {
                      let cur = out.length;
                      for (let j = 0; j < cur; j++) {
                        let newKey = Object.keys(out[j])[0];
                        let randomNumber = Math.floor(Math.random() * 100000);
                        newKey += randomNumber;
                        newKeys.push(newKey);
                        // res.push(out[j]);
                        global.distribution.local.store.put(out[j],
                          newKey,
                          (e, v) => {
                            if (e) {
                              console.log('error in put: ', e);
                              cb(e, null);
                            }
                            cur--;
                            if (cur === 0) {
                              count++;
                            }
                            console.log('put: ', keys[i],
                              out[j], ' count: ', count);
                            if (count === keys.length) {
                              cb(null, newKeys);
                            }
                          });
                      }
                    } else {
                      let newKey = Object.keys(out)[0];
                      let randomNumber = Math.floor(Math.random() * 100000);
                      newKey += randomNumber;
                      newKeys.push(newKey);
                      // res.push(out);
                      global.distribution.local.store.put(out, newKey, (e, v) => {
                        count++;
                        console.log('put: ', keys[i], out, ' count: ', count);
                        if (count === keys.length) {
                          cb(null, newKeys);
                        }
                      });
                    }
                  });
                }
                else {
                  out = map(keys[i], v);
                  out = compact(out);
                  console.log('out is ', out);
                  // console.log(i, out);
                  if (Array.isArray(out)) {
                    let cur = out.length;
                    for (let j = 0; j < cur; j++) {
                      let newKey = Object.keys(out[j])[0];
                      let randomNumber = Math.floor(Math.random() * 100000);
                      newKey += randomNumber;
                      newKeys.push(newKey);
                      // res.push(out[j]);
                      global.distribution.local.store.put(out[j],
                        newKey,
                        (e, v) => {
                          cur--;
                          if (cur === 0) {
                            count++;
                          }
                          console.log('put: ', keys[i],
                            out[j], ' count: ', count);
                          if (count === keys.length) {
                            cb(null, newKeys);
                          }
                        });
                    }
                  } else {
                    let newKey = Object.keys(out)[0];
                    let randomNumber = Math.floor(Math.random() * 100000);
                    newKey += randomNumber;
                    newKeys.push(newKey);
                    // res.push(out);
                    global.distribution.local.store.put(out, newKey, (e, v) => {
                      count++;
                      console.log('put: ', keys[i], out, ' count: ', count);
                      if (count === keys.length) {
                        cb(null, newKeys);
                      }
                    });
                  }
                }

              } else {
                count++;
                console.log('count: ', count, 'can not find: ', keys[i]);
                if (count === keys.length) {
                  cb(null, newKeys);
                }
              }
            });
        }
      };

      // wrap up the reduce function
      let reduceWrapper = function (keys, reduce, cb) {
        // use an list to store the result cause the current node might be
        // responsible for multiple keys
        if (reduce === null) {
          reduce = (key, values) => {
            let res = {};
            res[key] = values;
            return res;
          }
        }
        let res = [];
        let dict = {};
        let count = 0;
        console.log('reduce input keys are: ', keys);
        for (let i = 0; i < keys.length; i++) {
          global.distribution.local.store.get(keys[i], (e, v) => {
            // if the key exist in the current node
            if (e === null) {
              console.log('reduce find it ', keys[i], v);
              let key = Object.keys(v)[0];
              if (key in dict) {
                dict[key].push(v[key]);
              } else {
                dict[key] = [v[key]];
              }
              // res.push(reduce(key, [v[key], v[key]]));
              count++;
              // console.log(res);
              if (count === keys.length) {
                for (let key in dict) {
                  if (dict.hasOwnProperty(key)) {
                    res.push(reduce(key, dict[key]));
                  }
                }
                console.log('reduce result is: ', res);
                cb(null, res);
              }
            } else {
              count++;
              if (count === keys.length) {
                for (let key in dict) {
                  if (dict.hasOwnProperty(key)) {
                    res.push(reduce(key, dict[key]));
                  }
                }
                console.log(res);
                cb(null, res);
              }
            }
          });
        }
      };

      // shuffle
      let shuffle = function (keys, gid, cb) {
        let count = 0;
        res = [];
        global.distribution.local.groups.get(gid, (e, value) => {
          let id2Node = {};
          let tempKeys = Object.keys(value);
          for (let i = 0; i < tempKeys.length; i++) {
            id2Node[global.distribution.util.id.getID(value[tempKeys[i]])] =
              value[tempKeys[i]];
          }
          // console.log('group members are ', value);
          // console.log('group members are ', Object.keys(id2Node));
          // console.log('group members are ', id2Node);
          for (let i = 0; i < keys.length; i++) {
            global.distribution.local.store.get(keys[i], (e, content) => {
              // if the key exist in the current node
              if (e === null) {
                let randomNumber = Math.floor(Math.random() * 100000);
                let key = global.distribution.util.id.getID(
                  content + randomNumber);
                res.push(key);
                let realKey = Object.keys(content)[0];
                console.log('find it ', keys[i], content);
                console.log('real key is ', realKey);
                console.log('key is ', key);
                let targetId = global.distribution.util.id.consistentHash(
                  global.distribution.util.id.getID(realKey),
                  Object.keys(id2Node));
                let targetNode = id2Node[targetId];
                console.log('send to node is ', targetNode.port, realKey);
                let message = [content, key];
                let remote = {
                  service: 'store',
                  method: 'put',
                  node: targetNode,
                };
                global.distribution.local.comm.send(message, remote, (e, v) => {
                  count++;
                  console.log(i, 'shuffle store ', realKey, content);

                  if (count === keys.length) {
                    cb(null, res);
                  }
                });
              } else {
                count++;
                if (count === keys.length) {
                  cb(null, res);
                }
              }
            });
          }
        });
      };

      // use routes to create endpoints for the map and reduce functions at the
      // nodes in the group
      let service = { map: mapWrapper, reduce: reduceWrapper, shuffle: shuffle };

      // setup phase
      global.distribution[context.gid].routes.put(service, mrId, (e, v) => {
        // map phase
        console.log('map');
        let mapMessage = [keys, map, context.gid, compact];
        let mapRemote = { service: mrId, method: 'map' };
        global.distribution[context.gid].comm.send(
          mapMessage,
          mapRemote,
          (e, mapV) => {
            // shuffle phase
            if (e !== null && Object.keys(e).length > 0) {
              console.log('error in map phase');
              console.log(e);
              callback(e, null);
              return;
            }
            shuffleKeys = [];
            ks = Object.keys(mapV);
            for (let i = 0; i < ks.length; i++) {
              shuffleKeys = shuffleKeys.concat(mapV[ks[i]]);
            }

            console.log('shuffle');
            let shuffleMessage = [shuffleKeys, context.gid];
            let shuffleRemote = { service: mrId, method: 'shuffle' };
            global.distribution[context.gid].comm.send(
              shuffleMessage,
              shuffleRemote,
              (e, nv) => {
                newK = [];
                ks = Object.keys(nv);
                for (let i = 0; i < ks.length; i++) {
                  newK = newK.concat(nv[ks[i]]);
                }
                if (e !== null && Object.keys(e).length > 0) {
                  console.log('error in shuffle phase');
                  callback(e, null);
                  return;
                }
                console.log('new keys are ', newK);
                // reduce phase
                console.log('reduce');
                let reduceMessage = [newK, reduce];
                let reduceRemote = { service: mrId, method: 'reduce' };
                global.distribution[context.gid].comm.send(reduceMessage,
                  reduceRemote, (e, value) => {
                    // completion phase
                    if (e !== null && Object.keys(e).length > 0) {
                      console.log('error in reduce phase');
                      callback(e, null);
                      return;
                    }
                    console.log('reduce result returned is: ', value);
                    let finalRes = [];
                    for (let key in value) {
                      if (value.hasOwnProperty(key)) {
                        finalRes = finalRes.concat(value[key]);
                      }
                    }
                    console.log('final result is: ', finalRes);
                    for (let i = 0; i < finalRes.length; i++) {
                      for (let j = 0; j < Object.keys(finalRes[i]).length; j++) {
                        let key = Object.keys(finalRes[i])[j];
                        console.log('final results are ', key, finalRes[i][key]);
                      }
                    }
                    callback(null, finalRes);
                  });
              });
          });
      });
    },
  };
};

module.exports = mr;

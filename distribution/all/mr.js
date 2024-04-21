const id = require('../util/id');
const localComm = require('../local/comm');

// const statusCheck = () => {
//   console.log('start status check!');
//   let remote = {
//     service: 'status',
//     method: 'get',
//     node: {ip: '127.0.0.1', port: 7110},
//   };
//   localComm.send(['nid'], remote, (e, resultKey) => {
//     console.log('status: ', resultKey, e);
//   });

//   remote = {
//     service: 'status',
//     method: 'get',
//     node: {ip: '127.0.0.1', port: 7111},
//   };
//   localComm.send(['nid'], remote, (e, resultKey) => {
//     console.log('status: ', resultKey, e);
//   });

//   remote = {
//     service: 'status',
//     method: 'get',
//     node: {ip: '127.0.0.1', port: 7112},
//   };
//   localComm.send(['nid'], remote, (e, resultKey) => {
//     console.log('status: ', resultKey, e);
//   });
//   console.log('end status check!');
// };

const getSelectedNode = (key, nodes, context) => {
  const nids = Object.values(nodes).map((node) => id.getNID(node));
  const kid = id.getID(key);
  const selectedNid = context.hash(kid, nids);
  return nodes[selectedNid.substring(0, 5)];
};

const mr = function (config) {
  let context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || id.naiveHash;

  return {
    exec: (configuration, callback) => {
      mrService = {
        map: (key, gid, config, callback) => {
          callback = callback || function () { };
          const callMap = async () => {
            
            let value;
            try {
              value = await promisify(global.distribution[gid].store.get)(key);
            } catch (e) {
              console.error('Error getting value from store: ', e);
              throw e;
            }
            console.log('start processing key: ', key, 'value: ', value);
            if (config.map.constructor.name === 'AsyncFunction') {
              let result;
              try {
                result = await config.map(gid, value);
              } catch (e) {
                console.error('Error in map function: ', e);
                throw e;
              }
              if (config.compact) {
                result = config.compact(result);
              }
              console.log('end processing key: ', key, 'value: ', value);
              const resultKey = Object.keys(result)[0];
              const resultValue = result[resultKey];
              let promises = [];
              if (config.notShuffle) { 
                console.log('not shuffling!')
                resultValue.forEach((url) => {
                  promises.push(
                    global.promisify(global.distribution[gid].store.put)(url, global.distribution.util.id.getID(url))
                  );
                });
                Promise.all(promises)
                  .then((v) => {
                    return resultKey;
                  })
                  .catch((e) => {
                    throw e;
                  });
                return;
              }
              let storeGroup = config.storeGroup || gid;
              console.log('storeGroup: ', storeGroup);
              try {
                let value = await promisify(global.distribution[storeGroup].store.get)(resultKey);
                if (Array.isArray(resultValue)) {
                  value.push(...resultValue);
                } else {
                  value.push(resultValue);
                }
                result[resultKey] = value;
                console.log('added to exsiting list: ', result)
              } catch (e) {
                if (Array.isArray(resultValue)) {
                  result[resultKey] = resultValue;
                } else {
                  result[resultKey] = [resultValue];
                }
                console.log('creating a list!', result)
              }

              if (config.notStore) {
                return result[resultKey];
              } else {

                global.distribution[storeGroup].store.put(result[resultKey], resultKey, (e, v) => {
                  if (e) {
                    throw e;
                  } else {
                    console.log('store complete:', v.length);
                    console.log('store complete:', v);
                    return 'done';
                  }
                });

              }



            } else {
              try {
                let result = config.map(key, value);
                if (config.compact) {
                  result = config.compact(result);
                }
                console.log('end processing key: ', key, 'value: ', value);
                const resultKey = Object.keys(result)[0];
                const resultValue = result[resultKey];
                //shuffle
                global.distribution[gid].store.get(resultKey, (e, value) => {
                  console.log('shuffle phase, resultKey: ', resultKey, 'value: ', value, 'error: ', e);
                  if (e) {
                    result[resultKey] = [resultValue];
                    console.log('creating a list!')
                  } else {
                    value.push(resultValue);
                    result[resultKey] = value;
                    console.log('added to exsiting list: ', result)
                  }
                  console.log('value: ', value);
                  console.log('before shuffle put:', result[resultKey], resultKey);
                  if (config.notStore) {
                    return result[resultKey];
                  } else {
                    global.distribution[gid].store.put(result[resultKey], resultKey, (e, v) => {
                      console.log('store complete:', e, v)
                      if (e) {
                        throw e;
                      }
                    console.log('stored result: ', result);
                    return resultKey;  
                    });

                  }
                });
              } catch (e) {
                console.log('end processing key with ERRORR: ', key, 'value: ', value, e);
                throw e;
              }
            }

          }
          callMap().then((result) => {
            console.log('map success', result);
            callback(null, result);
          }).catch((e) => {
            callback(e, null);
          });


        },
        reduce: (key, gid, r, callback) => {
          console.log('reduce key before store.get: ', key);
          callback = callback || function () { };
          global.distribution[gid].store.get(key, (e, value) => {
            if (e) {
              console.log('error in reduce: ', e);
              callback(e, null);
            }
            try {
              console.log('reduce key: ', key, 'value: ', value);
              console.log('error in try', e);
              let result = r(key, value);
              console.log('reduce result in the infrastructure: ', result);
              global.distribution[gid].store.put(result, key, (e, result) => {
                if (e) {
                  console.log('error in reduce store.put: ', e);
                  callback(e, null);
                }
                callback(null, result);
              });
            } catch (e) {
              console.log('reduce error: ', e);
              callback(e, null);
            }
          });
        },
      };
      mrServiceName = 'mr-' + id.getSID(mrService);
      let nodes;
      global.distribution[context.gid].groups.get(context.gid,
        (e, prevNodes) => {
          console.log('e, prevNodes', e, prevNodes);
          // statusCheck();
          global.distribution[context.gid].routes
            .put(mrService, mrServiceName, (e, resultKey) => {
              console.log('Instatiation completed!', resultKey, e);
            });


          nodes = Object.values(prevNodes)[0];
          // statusCheck();
          let totalRequests = configuration.keys.length;
          console.log('totalRequests: ', totalRequests);
          // console.log('configuration.keys: ', configuration.keys);
          let completedRequests = 0;
          let errorsMap = {};
          let mapResultKeys = new Set();
          const checkAllDoneMap = () => {
            console.log('map completedRequests: ', completedRequests);
            if (completedRequests === totalRequests) {
              console.log('map errorsMap: ', errorsMap, Object.keys(errorsMap).length);
              if (Object.keys(errorsMap).length > 0) {
                console.log('found errors!')
                callback(errorsMap, null);
                return;
              }
              console.log('shuffled mapResultKeys: ', mapResultKeys);
              if (configuration.reduce === null) {
                // if there is no reduce,
                // we just distributed map result store
                // callback(null, mapResultKeys);
                callback(null, 'map phase done');
                return;
              }
              let totalRequestsReduce = mapResultKeys.size;
              let completedRequestsReduce = 0;
              console.log('totalRequestsReduce: ', totalRequestsReduce,
                'completedRequestsReduce:', completedRequestsReduce);
              let errorsReduce = [];
              let reduceResults = [];
              const checkAllDoneReduce = () => {
                console.log('completedRequestsReduce: ', completedRequestsReduce);
                if (completedRequestsReduce === totalRequestsReduce) {
                  console.log('reduced results: ', reduceResults);
                  callback(errorsReduce, reduceResults);
                }
              };
              var storeGroup = '';
              if (configuration.storeGroup) {
                storeGroup = configuration.storeGroup;
              } else {
                storeGroup = context.gid;
              }
              console.log('mapResultKeys before reduce: ', mapResultKeys);
              for (const key of mapResultKeys) {
                const selectedNode = getSelectedNode(key, nodes, context);
                let remote = {
                  service: mrServiceName,
                  method: 'reduce',
                  node: selectedNode,
                };
                console.log('reduce key before local comm: ', key);
                localComm.send([key, storeGroup,
                  configuration.reduce],
                  remote, (e, reduceResult) => {
                    if (e) {
                      errorsReduce.push(e);
                    } else {
                      console.log('each reduceResult: ', reduceResult);
                      reduceResults.push(reduceResult);
                    }
                    console.log('the final reduceResults: ', reduceResults);
                    completedRequestsReduce++;
                    checkAllDoneReduce();
                  });
              }
            }
          };
          console.log('Start mapping phase!, number of keys: ', configuration.keys.length);
          for (const key of configuration.keys) {
            console.log('calling map on key: ', key);
            const selectedNode = getSelectedNode(key, nodes, context);
            let remote = {
              service: mrServiceName,
              method: 'map',
              node: selectedNode,
            };

            const mapConfig = {
              map: configuration.map,
              compact: configuration.compact,
              notStore: configuration.notStore,
              notShuffle: configuration.notShuffle,
              storeGroup: configuration.storeGroup,
            }
            let args = [key, context.gid,
              mapConfig];
            console.log('map args: ', args);
            localComm.send(args, remote, (e, mapResultKey) => {
              if (e) {
                // errors.push(e);
                errorsMap[key] = e;
              } else {
                console.log('mapResultKey: ', mapResultKey, e);
                mapResultKeys.add(mapResultKey);
              }
              completedRequests++;
              checkAllDoneMap();
            });
          }
        });
    },
    deleteService: (serviceName, callback) => {
      console.log('deleting service: ', serviceName);
      let remote = { service: 'mr', method: 'deleteService' };
      global.distribution[context.gid].comm.send([serviceName]
        , remote, (e, v) => {
          console.log('delete services:::', e, v);
          callback(e, v);
        });
    },
  };
};

module.exports = mr;

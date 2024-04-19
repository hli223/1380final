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
        map: (key, gid, m, compact, callback) => {
          callback = callback || function () { };
          global.distribution[gid].store.get(key, (e, value) => {
            if (e) {
              callback(e, null);
            }
            console.log('start processing key: ', key, 'value: ', value);
            if (m.constructor.name === 'AsyncFunction') {
              m(key, value).then((result) => {
                if (compact) {
                  result = compact(result);
                }
                console.log('end processing key: ', key, 'value: ', value);
                callback(null, result);
              });
            } else {
              try {
                let result = m(key, value);
                if (compact) {
                  result = compact(result);
                }
                console.log('end processing key: ', key, 'value: ', value);
                const resultKey = Object.keys(result)[0];
                const resultValue = result[resultKey];
                //shuffle
                global.distribution[gid].store.get(resultKey, (e, value) => {
                  if (e) {
                    result[resultKey] = [resultValue];
                  } else {
                    result[resultKey].push(resultValue);
                  }
                  console.log('value: ', value);
                });
                global.distribution[gid].store.put(resultValue, resultKey, (e, resultKey) => {
                  if (e) {
                    callback(e, null);
                  }
                  callback(null, resultKey);
                });
                // callback(null, result);
              } catch (e) {
                console.log('end processing key with ERRORR: ', key, 'value: ', value, e);
                callback(e, null);
              }
            }
          });
        },
        reduce: (key, gid, r, callback) => {
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
              global.distribution[gid].store.put(result, key, (e, resultKey) => {
                if (e) {
                  console.log('error in reduce store.put: ', e);
                  callback(e, null);
                }
                callback(null, resultKey);
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
          let mapResultKeys = {};
          const checkAllDoneMap = () => {
            console.log('completedRequests: ', completedRequests);
            if (completedRequests === totalRequests) {
              if (Object.keys(errorsMap).length > 0) {
                callback(errorsMap, null);
                return;
              }
              console.log('shuffled mapResultKeys: ', mapResultKeys);
              if (configuration.reduce === null) {
                // if there is no reduce,
                // we just distributed map result store
                callback(null, mapResultKeys);
                return;
              }
              let totalRequestsReduce = Object.keys(mapResultKeys).length;
              let completedRequestsReduce = 0;
              console.log('totalRequestsReduce: ', totalRequestsReduce,
                'completedRequestsReduce:', completedRequestsReduce);
              let errorsReduce = [];
              let reduceResults = [];
              const checkAllDoneReduce = () => {
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
              for (const key of Object.keys(mapResults)) {
                const selectedNode = getSelectedNode(key, nodes, context);
                let remote = {
                  service: mrServiceName,
                  method: 'reduce',
                  node: selectedNode,
                };
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
          console.log('Start mapping phase!');
          for (const key of configuration.keys) {
            console.log('calling map on key: ', key);
            const selectedNode = getSelectedNode(key, nodes, context);
            let remote = {
              service: mrServiceName,
              method: 'map',
              node: selectedNode,
            };
            let args = [key, context.gid, configuration.map,
              configuration.compact];
            console.log('map args: ', args);
            localComm.send(args, remote, (e, mapResultKey) => {
              if (e) {
                // errors.push(e);
                errorsMap[key] = e;
              } else {
                console.log('mapResultKey: ', mapResultKey, e);
                mapResultKeys[key] = mapResultKey;
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

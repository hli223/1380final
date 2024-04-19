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
                callback(null, result);
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
          let mapResults = {};
          const checkAllDoneMap = () => {
            console.log('completedRequests: ', completedRequests);
            if (completedRequests === totalRequests) {
              if (Object.keys(errorsMap).length > 0) {
                callback(errorsMap, null);
                return;
              }
              console.log('shuffled mapResults: ', mapResults);
              if (configuration.notStore) {
                callback(null, mapResults);
                return;
              }
              let storePutCompletedRequests = 0;
              let storePutResult = [];
              let storePutErrors = {};
              const checkAllDoneStorePut = () => {
                console.log('shuffle key store not complete!',
                  storePutCompletedRequests,
                  Object.keys(mapResults).length);
                if (storePutCompletedRequests ===
                  Object.keys(mapResults).length) {
                  console.log('shuffle key store complete!',
                    storePutCompletedRequests,
                    Object.keys(mapResults).length);
                  if (Object.keys(storePutErrors).length > 0) {
                    callback(storePutErrors, null);
                    return;
                  }
                  if (configuration.reduce === null) {
                    // if there is no reduce,
                    // we just distributed map result store
                    callback(null, storePutResult);
                    return;
                  }

                  let totalRequestsReduce = Object.keys(mapResults).length;
                  let completedRequestsReduce = 0;
                  console.log('totalRequestsReduce: ', totalRequestsReduce,
                    'completedRequestsReduce:', completedRequestsReduce);
                  let errorsReduce = [];
                  let reduceResults = [];
                  const checkAllDoneReduce = () => {
                    if (completedRequestsReduce === totalRequestsReduce) {
                      console.log('reduced results: ', reduceResults);

                      // let totalRequestsDelete = Object
                      //   .keys(reduceResults).length;
                      // let completedRequestsDelete = 0;
                      // console.log('totalRequestsDelete: ',
                      //   totalRequestsDelete,
                      //   'completedRequestsDelete:',
                      //   completedRequestsDelete);
                      // let errorsDelete = [];
                      // const checkAllDoneDelete = () => {
                      //   if (completedRequestsDelete == totalRequestsDelete) {
                      //     console.log('delete completed! ');
                      //     // global.distribution[context.gid].
                      //     // mr.deleteService(mrServiceName, console.log);
                      //     callback(errorsDelete, reduceResults);
                      //   }
                      // };
                      // var storeGroup = '';
                      // if (configuration.storeGroup) {
                      //   storeGroup = configuration.storeGroup;
                      // } else {
                      //   storeGroup = context.gid;
                      // }
                      // reduceResults.forEach((reduceResult) => {
                      //   let key = Object.keys(reduceResult)[0];
                      //   global.distribution[storeGroup]
                      //     .store.del(key, (e, resultKey) => {
                      //       if (e) {
                      //         errorsDelete.push(e);
                      //       }
                      //       completedRequestsDelete++;
                      //       console.log('deleting key: ', key);
                      //       checkAllDoneDelete();
                      //     });
                      // });
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
              var storeGroup = '';
              if (configuration.storeGroup) {
                storeGroup = configuration.storeGroup;
              } else {
                storeGroup = context.gid;
              }
              for (const key of Object.keys(mapResults)) {
                global.distribution[storeGroup].
                  store.put(mapResults[key], key, (e, resultKey) => {
                    if (e) {
                      storePutErrors[key] = e;
                    }
                    console.log('shuffle store put error: ', e);
                    storePutCompletedRequests++;
                    storePutResult.push(resultKey);
                    checkAllDoneStorePut();
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
            localComm.send(args, remote, (e, mapResult) => {
              if (e) {
                // errors.push(e);
                errorsMap[key] = e;
              } else {
                console.log('mapResult: ', mapResult, e);
                if (Array.isArray(mapResult)) {
                  mapResult.forEach((element) => {
                    const key = Object.keys(element)[0];
                    console.log('before shuffle key: ', key);
                    console.log('before shuffle element: ', element[key])
                    if (!(key in mapResults)) {
                      mapResults[key] = [element[key]];
                    } else {
                      mapResults[key].push(element[key]);
                    }
                  });
                } else {
                  for (const key of Object.keys(mapResult)) {
                    if (!(key in mapResults)) {
                      mapResults[key] = [mapResult[key]];
                    } else {
                      mapResults[key].push(mapResult[key]);
                    }
                  }
                }
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

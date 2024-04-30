const id = require('../util/id');
const localComm = require('../local/comm');


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
  let mrServiceExists = false;
  let prevNodes = null;

  return {
    exec: (configuration, callback) => {
      mrService = {
        map: (keys, gid, config, callback) => {
          callback = callback || function () { };
          console.log('map keys: ', keys);
          const callMap = async (key) => {//return resultKey
            let value;
            try {
              value = await global.promisify(global.distribution[gid].store.get)(key);
            } catch (e) {
              console.error('Error getting value from store: ', e);
              throw e;
            }
            console.log('start processing key: ', key, 'value: ', value);
            let result;
            if (config.map.constructor.name === 'AsyncFunction') {
              try {
                result = await config.map(gid, value);
              } catch (e) {
                console.error('Error in map function: ', e);
                throw e;
              }
            } else {
              result = config.map(key, value);
              if (config.compact) {
                result = config.compact(result);
              }
            }
              if (config.compact) {
                result = config.compact(result);
              }
              console.log('end processing key: ', key, 'value: ', value);
              console.log('user defined map result: ', result);
              let storeGroup = config.storeGroup || gid;
              console.log('storeGroup: ', storeGroup);
              const resultKey = Object.keys(result)[0];
              const resultValue = result[resultKey];
              console.log('resultValue: ', resultValue);
              let promises = [];
              if (config.notShuffle) { 
                console.log('not shuffling!')
                for (const url of resultValue) {
                  try {
                    await global.promisify(global.distribution[gid].store.put)(url, global.distribution.util.id.getID(url));
                  } catch (e) {
                    throw e;
                  }
                }
                return resultKey;
              }
              if (config.notStore) {
                return result[resultKey];
              }
              //shuffle
              // console.log('start shuffle!', result, Object.keys(result))
              let intermediateStore = config.intermediateStore;
              for (const resultKey of Object.keys(result)) {
                try {
                  console.log('store to ' + intermediateStore + '!', resultKey, result[resultKey]);
                  const v = await global.promisify(global.distribution[storeGroup][intermediateStore].put)(result[resultKey], resultKey);
                  console.log('store ' + intermediateStore + ' complete:', resultKey);
                } catch (e) {
                  console.log('error in store to ' + intermediateStore + '!', e);
                  throw e;
                }
              }
              return Object.keys(result);

          }

          Promise.all(keys.map(key => callMap(key)))
            .then(() => {
              callback(null, 'map phase done');
            })
            .catch((e) => {
              callback(e, null);
            });


        },
        reduce: (keys, gid, config, callback) => {
          console.log('reduce keys before store.get: ', keys);
          let storeGroup = config.storeGroup || gid;  
          callback = callback || function () { };
          const callReduce = async (key) => {//return resultKey
            let value;
            try {
              value = await global.promisify(global.distribution[storeGroup].mem.get)(key);
              console.log('successfully getting value for key: ', key, 'value: ', value);
            } catch (e) {
              console.error('Error getting value from mem for key: ', key, 'error: ', e, e.stack);
              throw e;
            }

            let result;
            result = config.reduce(key, value);
            
            const resultKey = Object.keys(result)[0];
            const resultValue = result[resultKey];


            //store append to store final output
            try {
              const v = await global.promisify(global.distribution[storeGroup].store.put)(resultValue, resultKey);
              console.log('reduce store complete: ', resultKey, v);
              return resultKey;
            } catch (e) {
              throw e;
            }

          }

          Promise.all(keys.map(key => callReduce(key)))
            .then(async (results) => {
              console.log('reduce success', results);
              callback(null, 'reduce phase done');
            })
            .catch((e) => {
              console.error('reduce error within reduce: ', e);
              callback(e, null);
            });
        },
      };
      mrServiceName = 'mr-' + id.getSID(mrService);
      const doMapReduce = async () => {

        if (prevNodes === null) {
          try {
            prevNodes = await global.promisify(global.distribution[context.gid].groups.get)(context.gid);
            console.log('prevNodes in invert index: ', prevNodes);
          } catch (e) {
            console.log('error in getting prevNodes for invert index: ', e);
            throw e;
          }
        }

        if (!mrServiceExists) {
          try {
            let resultKey = await global.promisify(global.distribution[context.gid].routes.put)(mrService, mrServiceName);
            console.log('Instatiation completed!', resultKey);
            mrServiceExists = true;
          } catch (e) {
            console.log('Error in promisifying and putting route: ', e);
            throw e;
          }
        }

        let nodes = Object.values(prevNodes)[0];
        let numNodes = Object.keys(nodes).length;
        console.log('numNodes: ', numNodes, nodes);
        // statusCheck();
        let totalRequests = configuration.keys.length;
        console.log('totalRequests: ', totalRequests);
        console.log('Start mapping phase!, number of keys: ', configuration.keys.length);

        //splitting data into shards
        const splitDataKeysIntoShards = (keys) => {
          let keySublists = [];
          if (keys.length < numNodes) {
            keySublists = keys.map(key => [key]);
          } else {
            let keysPerNode = Math.ceil(keys.length / numNodes);
            for (let i = 0; i < keys.length; i += keysPerNode) {
              keySublists.push(keys.slice(i, Math.min(i + keysPerNode, keys.length)));
            }

          }
          return keySublists;
        }
        let keySublists = splitDataKeysIntoShards(configuration.keys);

        console.log('Map: length of keySublists: ', keySublists.length, 'number of elements in keySublists: ', keySublists.reduce((total, sublist) => total + sublist.length, 0), 'number of keys: ', configuration.keys.length);

        let mapPromises = [];
        let mapConfig = {
          map: configuration.map,
          compact: configuration.compact,
          notStore: configuration.notStore,
          notShuffle: configuration.notShuffle,
          storeGroup: configuration.storeGroup,
          intermediateStore: configuration.intermediateStore || 'mem'
        }

        for (let i = 0; i < keySublists.length; i++) {
          const keySublist = keySublists[i];
          console.log('calling map on keys: ', keySublist);
          const selectedNode = nodes[Object.keys(nodes)[i]];
          let remote = {
            service: mrServiceName,
            method: 'map',
            node: selectedNode,
          };

          let args = [keySublist, context.gid,
            mapConfig];
          console.log('map args: ', args);
          
          mapPromises.push(global.promisify(localComm.send)(args, remote));
        }
        try {
          let mapResults = await Promise.all(mapPromises);
          console.log('map results: ', mapResults);
          if (mapResults.some(result => result instanceof Error)) {//we encounter error in map phase
            return mapResults;
          }
          if (configuration.reduce===null) {
            return mapResults;
          }
          let resultKeys;
          try {
            console.log('fetching resultKeys from ', mapConfig.intermediateStore);
            resultKeys = await global.promisify(global.distribution[context.gid][mapConfig.intermediateStore].get)(null);
            console.log('Fetched resultKeys from memory: ', resultKeys);
          } catch (e) {
            console.error('Error fetching resultKeys from memory: ', e);
            throw e;
          }
          keySublists = splitDataKeysIntoShards(resultKeys);
          console.log('Reduce: length of keySublists: ', keySublists.length, 'number of elements in keySublists: ', keySublists.reduce((total, sublist) => total + sublist.length, 0), 'number of keys: ', resultKeys.length);

          let reducePromises = [];
          for (let i = 0; i < keySublists.length; i++) {
            const keySublist = keySublists[i];
            console.log('calling reduce on keys: ', keySublist);
            const selectedNode = nodes[Object.keys(nodes)[i]];
            let remote = {
              service: mrServiceName,
              method: 'reduce',
              node: selectedNode,
            };
            const reduceConfig = {
              reduce: configuration.reduce,
              storeGroup: configuration.storeGroup,
            }
            let reduceBatchSize = 15;
            if (keySublist.length > reduceBatchSize) {
              let batches = [];
              for (let j = 0; j < keySublist.length; j += reduceBatchSize) {
                batches.push(keySublist.slice(j, j + reduceBatchSize > keySublist.length ? keySublist.length : j + reduceBatchSize));
              }
              for (let batch of batches) {
                let args = [batch, context.gid, reduceConfig];
                console.log('reduce args: ', args);
                console.log('reduce node: ', selectedNode);
                reducePromises.push(global.promisify(localComm.send)(args, remote));
                await new Promise(resolve => setTimeout(resolve, 200));
              }
            } else {
              let args = [keySublist, context.gid, reduceConfig];
              console.log('reduce args: ', args);
              console.log('reduce node: ', selectedNode);
              reducePromises.push(global.promisify(localComm.send)(args, remote));
            }
          }

          try {
            let resultKeys = await Promise.all(reducePromises);
            console.log('reduce results: ', resultKeys);
            resultKeys = resultKeys.flat(Infinity);
            console.log('flat reduce results: ', resultKeys);
            return resultKeys;
          } catch (e) {
            console.log('reduce error: ', e);
            throw e;
          }

        } catch (e) {
          console.log('map error: ', e);
          throw e;
        }





      }

      doMapReduce().then(async (result) => {
        console.log('mapReduce result: ', result);
        const clearResult = await global.promisify(global.distribution[context.gid].mem.clear)();
        console.log('Clear operation result:', clearResult);
        callback(null, result);
      }).catch((e) => {
        callback(e, null);
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

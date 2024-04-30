const id = require('../util/id');
const localComm = require('../local/comm');
const { promisify } = require('util');

let mem = (config) => {
  let context = {};
  context.gid = config.gid || 'all';

  context.hash = config.hash || id.naiveHash;
  let perNodeViews = null;

  return {
    get: async (key, callback) => {
      callback = callback || function() {};
      if (perNodeViews === null) {
        try {
            perNodeViews = await global.promisify(global.distribution[context.gid].groups.get)(context.gid);
        } catch (e) {
            callback(e, null);
            return;
        }
      }

            nodes = Object.values(perNodeViews)[0];
            if (key === null) {
              let totalRequests = Object.keys(nodes).length;
              let completedRequests = 0;
              let errors = [];
              let values = [];

              const checkAllDone = () => {
                if (completedRequests === totalRequests) {
                  if (errors.length > 0) {
                    callback({}, null);
                  } else {
                    callback({}, values);
                  }
                }
              };
              for (const node of Object.values(nodes)) {
                let remote = {
                  service: 'mem',
                  method: 'get',
                  node: node,
                };
                let args = [{key: key, gid: context.gid}];
                try {
                  const vPerNode = await global.promisify(localComm.send)(args, remote);
                  console.log('vPerNode', vPerNode);
                  values = values.concat(vPerNode);
                  completedRequests++;
                } catch (e) {
                  errors.push(e);
                  completedRequests++;
                }
                checkAllDone();
              }
            } else {
                console.log('all mem get: ', key);
              const nids = Object.values(nodes).map((node) => id.getNID(node));
              const kid = id.getID(key);
              const selectedNid = context.hash(kid, nids);
              const selectedNode = nodes[selectedNid.substring(0, 5)];
              let remote = {
                service: 'mem',
                method: 'get',
                node: selectedNode,
              };
              let args = [{key: key, gid: context.gid}];
              await new Promise(resolve => setTimeout(resolve, 50));
              try {
                const v = await global.promisify(localComm.send)(args, remote);
                console.log('all mem get success: ', key, v);
                callback(null, v);
              } catch (e) {
                console.error('all mem get error: ', key, e, e.stack);
                callback(e, null);
              }
            }

    },
    put: async (value, key, callback) => {
      callback = callback || function() {};
      if (key === null) {
        key = id.getID(value);
      }
      if (perNodeViews === null) {
        try {
            perNodeViews = await global.promisify(global.distribution[context.gid].groups.get)(context.gid);
        } catch (e) {
            callback(e, null);
            return;
        }
      }
            // if (Object.keys(e).length!==0) {
            //   callback(e, null);
            //   return;
            // }
            nodes = Object.values(perNodeViews)[0];
            const nids = Object.values(nodes).map((node) => id.getNID(node));
            const kid = id.getID(key);
            const selectedNid = context.hash(kid, nids);
            const selectedNode = nodes[selectedNid.substring(0, 5)];

            let remote = {
              service: 'mem',
              method: 'put',
              node: selectedNode,
            };
            console.log('all mem put: ', key, value);

            let args = [value, {key: key, gid: context.gid}];
            try {
                const v = await global.promisify(localComm.send)(args, remote);
                callback(null, v);
            } catch (e) {
                callback(e, null);
            }

    },
    del: async (key, callback) => {
      callback = callback || function() {};
      global.distribution[context.gid].groups.get(context.gid,
          (e, perNodeViews) => {
            let selectedNode;
            if (typeof key !== 'string') {
              selectedNode = key[1];
              key = key[0];
            } else {
              nodes = Object.values(perNodeViews)[0];
              const nids = Object.values(nodes).map((node) => id.getNID(node));
              const kid = id.getID(key);
              const selectedNid = context.hash(kid, nids);
              selectedNode = nodes[selectedNid.substring(0, 5)];
            }
            let remote = {
              service: 'mem',
              method: 'del',
              node: selectedNode,
            };
            let args = [{key: key, gid: context.gid}];
            localComm.send(args, remote, (e, v) => {
              if (e) {
                callback(e, null);
                return;
              }
              callback(null, v);
            });
          });
    },
    clear: async (callback) => {
    callback = callback || function() {};
      if (perNodeViews === null) {
        try {
            perNodeViews = await global.promisify(global.distribution[context.gid].groups.get)(context.gid);
        } catch (e) {
            callback(e, null);
            return;
        }
      }
            let nodes = Object.values(perNodeViews)[0];
            let totalNodes = Object.keys(nodes).length;
            let completedNodes = 0;
            let errors = [];

            Object.values(nodes).forEach(node => {
            let remote = {
                service: 'mem',
                method: 'clear',
                node: node,
            };
            localComm.send([], remote, (err) => {
                if (err) {
                errors.push(err);
                }
                completedNodes++;
                if (completedNodes === totalNodes) {
                if (errors.length > 0) {
                    callback({ message: "Errors occurred during clearing", errors: errors }, null);
                } else {
                    callback(null, { message: "All nodes cleared" });
                }
                }
            });
            });

    },
    reconf: (preGroup, callback) => {
      callback = callback || function() {};

      global.distribution[context.gid].groups.get(context.gid,
          (e, perNodeViews) => {
            if (Object.keys(e).length!==0) {
              callback(e, null);
              return;
            }
            reconfedNodes = Object.values(perNodeViews)[0];
            const newNids = Object.values(reconfedNodes)
                .map((node) => id.getNID(node));
            const oldNids = Object.values(preGroup)
                .map((node) => id.getNID(node));
            console.log('newNids', newNids);
            console.log('oldNids', oldNids);


            global.distribution[context.gid].mem.get(null, (e, allKeys) => {
              let completedOperations = 0;
              const totalOperations = allKeys.length;
              if (totalOperations === 0) {
                callback(null, 'No keys to reconfigure');
                return;
              }

              allKeys.forEach((key) => {
                const kid = id.getID(key);
                const oldNodeHash = context
                    .hash(kid, oldNids);
                const newNodeHash = context
                    .hash(kid, newNids);

                if (newNids.includes(oldNodeHash) &&
                  oldNodeHash !== newNodeHash) {
                  const oldNode = Object.values(preGroup)
                      .find((node) => id.getNID(node) === oldNodeHash);
                  global.distribution[context.gid].mem
                      .del([key, oldNode], (delError, value) => {
                        if (delError) {
                          callback(delError, null);
                          return;
                        }
                        global.distribution[context.gid].mem.put(value,
                            key, (putError) => {
                              completedOperations++;
                              if (putError) {
                                callback(putError, null);
                                return;
                              }

                              if (completedOperations === totalOperations) {
                                callback(null, 'Reconfiguration complete');
                              }
                            });
                      });
                } else {
                  completedOperations++;
                  if (completedOperations === totalOperations) {
                    callback(null, 'Reconfiguration complete');
                  }
                }
              });
            });
          });
    },
  };
};
module.exports = mem;


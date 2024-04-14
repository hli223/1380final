/* eslint-disable */
const id = require('../util/id');
const serialization = require('../util/serialization');

global.toLocal = new Map();

function createRPC(func) {
    let funcID = id.getID(serialization.serialize(func));
    global.toLocal.set(funcID, func);

    let functionSrc = `
        const callback = args.pop() || function() {};

        let remote = {node: ${JSON.stringify(global.nodeConfig)}, service:
'${funcID}', method: 'call'};
        let message = args;

        distribution.local.comm.send(message, remote, (error, response) => {
            if (error) {
                callback(error)
            } else {
                callback(null, response);
            }
        });
    `;

    return new Function('...args', functionSrc);
}


function toAsync(func) {
    return function(...args) {
      const callback = args.pop() || function() {};
      try {
        const result = func(...args);
        callback(null, result);
      } catch (error) {
        callback(error);
      }
    };
  }
  
  module.exports = {
    createRPC: createRPC,
    toAsync: toAsync,
  };
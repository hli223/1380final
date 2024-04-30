const http = require('http');
const serialization = require('../util/serialization');
const comm = {
  send: function (message, options, callback) {
    const helpers = {
      'executeFunction': function (func, ...args) {
        return func(...args);
      },
      'endEvent': 'end',
      'errorEvent': "error",
      'throwError': function (error, message) {
        return error(message);
      },
      'checkInstance': function (object, type) {
        return object instanceof type;
      },
      'errorMessage': "Message must be an array",
      'concatenate': function (str1, str2) {
        return str1 + str2;
      },
      'add': function (num1, num2) {
        return num1 + num2;
      },
      'httpMethod': "PUT",
      'contentType': "application/json"
    };
    if (!Array.isArray(message)) {
      throw new Error("Message must be an array");
    }
    let node = options.node;
    let service = options.service;
    let method = options.method;
    let serializedMessage = serialization.serialize(message);
    const requestOptions = {
      'hostname': node.ip,
      'port': node.port,
      'path': '/' + service + '/' + method,
      'method': "PUT",
      'headers': {
        'Content-Type': "application/json",
        'Content-Length': serializedMessage.length
        // serializedMessage.length
      }
    };
    const request = http.request(requestOptions, response => {
      const responseHandlers = {
        'handleCallback': function (func, ...args) {
          return helpers.executeFunction(func, ...args);
        }
      };
      let responseData = '';
      response.on("data", function (chunk) {
        responseData += chunk;
      });
      response.on('end', function () {
        if (callback) {
          responseHandlers.handleCallback(callback, ...serialization.deserialize(responseData));
        }
      });
      response.on("error", function (error) {
        if (callback) {
          responseHandlers.handleCallback(callback, new Error("Error on response"));
        }
      });
    });
    request.on("error", function (error) {
      if (callback) {
        console.log('error in local comm', error, 'message: ', message, 'options:', options);
        callback(new Error(error));
      }
    });
    request.write(serializedMessage);
    request.end();
  }
};
module.exports = comm;
const fs = require('fs');
const http = require('http');
const https = require('https');
const url = require('url');
const path = require('path');
const os = require('os');
const events = require('events');
const stream = require('stream');
const util = require('util');
const querystring = require('querystring');
const zlib = require('zlib');
const buffer = require('buffer');
const childProcess = require('child_process');
const cluster = require('cluster');
const dgram = require('dgram');
const dns = require('dns');
const http2 = require('http2');
const v8 = require('v8');

// private variables
let obj2Id = new Map();
let id2Obj = new Map();
let visited = new Set();
let first = true;
let nextId = 1;
let nativeFunctionsMap = new Map();
createNativeFunctionMap(globalThis);
createNativeFunctionMap(globalThis.global);
createNativeFunctionMap(globalThis.global.console);
nativeFunctionsMap.set(globalThis.String.name, globalThis.String);
nativeFunctionsMap.set(globalThis.Number.name, globalThis.Number);
nativeFunctionsMap.set(globalThis.Boolean.name, globalThis.Boolean);
nativeFunctionsMap.set(globalThis.Object.name, globalThis.Object);
nativeFunctionsMap.set(globalThis.Array.name, globalThis.Array);
nativeFunctionsMap.set(globalThis.RegExp.name, globalThis.RegExp);
nativeFunctionsMap.set(globalThis.Function.name, globalThis.Function);
nativeFunctionsMap.set(globalThis.Date.name, globalThis.Date);
// console.log(globalThis.Array.toString());
// console.log(nativeFunctionsMap.get(globalThis.Array.name).toString());
// console.log(typeof nativeFunctionsMap[globalThis.Array.name]);
createNativeFunctionMap(fs);
createNativeFunctionMap(http);
createNativeFunctionMap(https);
createNativeFunctionMap(url);
createNativeFunctionMap(path);
createNativeFunctionMap(events);
createNativeFunctionMap(stream);
createNativeFunctionMap(os);
createNativeFunctionMap(util);
createNativeFunctionMap(querystring);
createNativeFunctionMap(zlib);
createNativeFunctionMap(buffer);
createNativeFunctionMap(zlib);
createNativeFunctionMap(childProcess);
createNativeFunctionMap(cluster);
createNativeFunctionMap(dgram);
createNativeFunctionMap(dns);
createNativeFunctionMap(http2);
createNativeFunctionMap(v8);


// private functions
function createNativeFunctionMap(input) {
  for (const key in input) {
    if (input.hasOwnProperty(key)) {
      const value = input[key];
      if (typeof value === 'function' &&
        value.toString().includes('[native code]')) {
        nativeFunctionsMap.set(key, value);
      }
    }
  }
}

function serializeArray(input) {
  // input is an array
  // convert the input array into an object.
  // [1, 2, 3] --> {type: "Array", value: "[1, 2, 3]"}
  const output = {};
  output.type = 'Array';
  const children = [];
  input.forEach((element) => {
    children.push(serialize(element));
  });
  output.value = JSON.stringify(children);
  // console.log(output);
  return output;
}

function deserializeArray(input) {
  // input is an object with type of array.
  // extract the value.
  // {type: "Array", value: "[1, 2, 3]"} --> [1, 2, 3]
  const output = JSON.parse(input.value);
  const result = [];
  output.forEach((element) => {
    result.push(deserialize(element));
  });
  return result;
}

function serializeObj(input) {
  // input is an object
  // convert the input object into an object with required attribute
  const output = {};
  // put the input object into obj2Id and id2Obj
  obj2Id.set(input, nextId++);
  output.id = obj2Id.get(input).toString();
  // console.log(input, output.id);
  visited.add(input);
  output.type = 'Object';
  const attributes = {};
  for (let prop in input) {
    if (input.hasOwnProperty(prop)) {
      // if input[prop] points to an object that has
      // been serialized thus forming a cycle
      if (visited.has(input[prop])) {
        let temp = {};
        temp.type = 'Pointer';
        temp.value = output.id;
        attributes[prop] = JSON.stringify(temp);
        // console.log(temp);
      } else {
        attributes[prop] = serialize(input[prop]);
      }
    }
  }
  output.value = JSON.stringify(attributes);
  visited.delete(input);
  return output;
}

function deserializeObj(input) {
  // input is an object
  // extract the value into object
  const output = JSON.parse(input.value);
  const result = {};
  id2Obj.set(input.id, result);
  for (let prop in output) {
    if (output.hasOwnProperty(prop)) {
      result[prop] = deserialize(output[prop]);
    }
  };
  return result;
}


function serializeFunc(input) {
  // input is a function
  // convert the input into string and put it into object
  const output = {};
  output.type = 'Function';
  output.value = input.toString();
  return output;
}

function deserializeFunc(input) {
  // input is an object
  // convert the value into function
  return eval('(' + input.value + ')');
  // return eval(input.value);
}

function serializeErr(input) {
  // input is an error
  const output = {};
  output.type = 'Error';
  output.value = JSON.stringify(input, Object.getOwnPropertyNames(input), 2);
  return output;
}

function deserializeErr(input) {
  const error = new Error();
  Object.assign(error, JSON.parse(input.value));
  return error;
}

function serializeDate(input) {
  const output = {};
  output.type = 'Date';
  output.value = JSON.stringify(input);
  return output;
}

function deserializeDate(input) {
  return new Date(JSON.parse(input.value));
}


// public functions
function serialize(object) {
  // transfer the object into an object based on the type
  // and then JSON.stringify it into string.
  // base structures
  let output = {};
  if (typeof object === 'number') {
    output.type = 'Number';
    output.value = object.toString();
  } else if (typeof object === 'string') {
    output.type = 'String';
    output.value = object;
  } else if (typeof object === 'boolean') {
    output.type = 'Boolean';
    if (object) {
      output.value = 'true';
    } else {
      output.value = '';
    }
  } else if (object === null) {
    output.type = 'null';
  } else if (object === undefined) {
    output.type = 'undefined';
  } else if (object instanceof Array) {
    output = serializeArray(object);
  } else if (typeof object === 'function') {
    if (object.toString().includes('[native code]')) {
      output.type = 'Native';
      output.value = object.name;
      // console.log("found it");
    } else {
      output = serializeFunc(object);
    }
  } else if (typeof object === 'object') {
    if (object instanceof Error) {
      output = serializeErr(object);
    } else if (object instanceof Date) {
      output = serializeDate(object);
    } else {
      // whether it is the uppest level of object
      if (first) {
        first = false;
        nextId = 1;
        obj2Id = new Map();
        output = serializeObj(object);
        first = true;
      } else {
        output = serializeObj(object);
      }
    }
  }

  return JSON.stringify(output);
}

function deserialize(string) {
  // JSON.parse the string into an object
  // and then transform the input into it original type
  // based on the meta data.
  // console.log(string);
  // createNativeFunctionMap();
  let input = JSON.parse(string);
  if (input.type === 'Number') {
    return +input.value;
  } else if (input.type === 'String') {
    return input.value;
  } else if (input.type === 'Boolean') {
    return Boolean(input.value);
  } else if (input.type === 'null') {
    return null;
  } else if (input.type === 'undefined') {
    return undefined;
  } else if (input.type === 'Array') {
    return deserializeArray(input);
  } else if (input.type === 'Native') {
    // console.log("input value: ", input.value);
    // console.log(nativeFunctionsMap.get(input.value));
    return nativeFunctionsMap.get(input.value);
  } else if (input.type === 'Function') {
    return deserializeFunc(input);
  } else if (input.type === 'Error') {
    return deserializeErr(input);
  } else if (input.type === 'Date') {
    return deserializeDate(input);
  } else if (input.type === 'Object') {
    if (first) {
      first = false;
      id2Obj = new Map();
      const result = deserializeObj(input);
      first = true;
      return result;
    } else {
      return deserializeObj(input);
    }
  } else if (input.type === 'Pointer') {
    // console.log(input);
    // console.log(id2Obj);
    return id2Obj.get(input.value);
  }
  return {};
}

module.exports = {
  serialize: serialize,
  deserialize: deserialize,
};


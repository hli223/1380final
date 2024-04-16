global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');
global.fetch = require('node-fetch');

const ncdcGroup = {};
const dlibGroup = {};
const invertedIdxGroup = {};
const sourceSinkGroup = {};
const test1Group = {};

/*
   This hack is necessary since we can not
   gracefully stop the local listening node.
   The process that node is
   running in is the actual jest process
*/
let localServer = null;

/*
    The local node will be the orchestrator.
*/

const n1 = {ip: '127.0.0.1', port: 7110};
const n2 = {ip: '127.0.0.1', port: 7111};
const n3 = {ip: '127.0.0.1', port: 7112};

beforeAll((done) => {
  /* Stop the nodes if they are running */

  ncdcGroup[id.getSID(n1)] = n1;
  ncdcGroup[id.getSID(n2)] = n2;
  ncdcGroup[id.getSID(n3)] = n3;

  dlibGroup[id.getSID(n1)] = n1;
  dlibGroup[id.getSID(n2)] = n2;
  dlibGroup[id.getSID(n3)] = n3;

  invertedIdxGroup[id.getSID(n1)] = n1;
  invertedIdxGroup[id.getSID(n2)] = n2;
  invertedIdxGroup[id.getSID(n3)] = n3;

  sourceSinkGroup[id.getSID(n1)] = n1;
  sourceSinkGroup[id.getSID(n2)] = n2;
  sourceSinkGroup[id.getSID(n3)] = n3;

  test1Group[id.getSID(n1)] = n1;
  test1Group[id.getSID(n2)] = n2;
  test1Group[id.getSID(n3)] = n3;


  const startNodes = (cb) => {
    distribution.local.status.spawn(n1, (e, v) => {
      distribution.local.status.spawn(n2, (e, v) => {
        distribution.local.status.spawn(n3, (e, v) => {
          cb();
        });
      });
    });
  };

  distribution.node.start((server) => {
    localServer = server;

    const ncdcConfig = {gid: 'ncdc'};
    startNodes(() => {
      groupsTemplate(ncdcConfig).put(ncdcConfig, ncdcGroup, (e, v) => {
        const dlibConfig = {gid: 'dlib'};
        groupsTemplate(dlibConfig).put(dlibConfig, dlibGroup, (e, v) => {
          const invertedIdxConfig = {gid: 'invertedIdx'};
          groupsTemplate(invertedIdxConfig).
              put(invertedIdxConfig, invertedIdxGroup, (e, v) => {
                const sourceSinkConfig = {gid: 'sourceSink'};
                groupsTemplate(sourceSinkConfig).
                    put(sourceSinkConfig, sourceSinkGroup, (e, v) => {
                      const test1Config = {gid: 'test1'};
                      groupsTemplate(test1Config).
                          put(test1Config, test1Group, (e, v) => {
                            done();
                          });
                    });
              });
        });
      });
    });
  });
});

afterAll((done) => {
  let remote = {service: 'status', method: 'stop'};
  remote.node = n1;
  distribution.local.comm.send([], remote, (e, v) => {
    remote.node = n2;
    distribution.local.comm.send([], remote, (e, v) => {
      remote.node = n3;
      distribution.local.comm.send([], remote, (e, v) => {
        localServer.close();
        done();
      });
    });
  });
});

test('(25 pts) crawler workflow', (done) => {
  let m1 = async (key, url) => {
    let out = {};
    try {
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
      const response = await global.fetch(url);
      // const response = {
      //   ok: true,
      //   status: 200,
      //   text: () => {
      //     return '<html><body>Hello, world!</body></html>';
      //   }
      // }
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      var htmlContent = await response.text();
      htmlContent = htmlContent.replace("\u00a9", "&copy;")
      out[url] = htmlContent;
    } catch (error) {
      console.error(url+'Fetch error: ', error);
      out[url] = 'Error fetching URL: '+url + ' ' + error;
    }
    return out;
  };

  let dataset = [
    // {'5244': 'http://www.maxdml.com/'},
    // {
    //   '1235': 'https://cs0320.github.io/#assignments',
    // },
    // {'3425': 'https://google.com'},
    {'424': 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/level_2a/'},
    {'421': 'https://cs.brown.edu/courses/csci1380/sandbox/1'},
    // {'243': 'https://atlas.cs.brown.edu/data/dblp/'}
  ];

  /* Sanity check: map and reduce locally */
  // sanityCheck(m1, r1, dataset, expected, done);

  /* Now we do the same thing but on the cluster */
  const doMapReduce = (cb) => {
    distribution.ncdc.store.get(null, (e, v) => {
      try {
        console.log('keys v', v)
        expect(v.length).toBe(dataset.length);
      } catch (e) {
        done(e);
      }

      distribution.ncdc.mr.exec({keys: v, map: m1, reduce: null}, (e, v) => {
        try {
          // expect(v).toEqual(expect.arrayContaining(expected));
          done();
        } catch (e) {
          done(e);
        }
      });
    });
  };

  let cntr = 0;

  // We send the dataset to the cluster
  dataset.forEach((o) => {
    let key = Object.keys(o)[0];
    let value = o[key];
    distribution.ncdc.store.put(value, key, (e, v) => {
      cntr++;
      // Once we are done, run the map reduce
      console.log('put dataset:', v)
      if (cntr === dataset.length) {
        doMapReduce();
      }
    });
  });
  // m1('000', dataset[1]['106']).then((res) => {
  //   console.log(res);
  //   done();
  // });
});

// test('(25 pts) Distributed string matching workflow', (done) => {
//   let m1 = (key, string) => {
//     const regex = new RegExp('abc');
//     let out = {};
//     const outputKey = global.distribution.util.id.getID(string);
//     try {
//       if (regex.test(string)) {
//         out[outputKey] = key;
//       }
//     } catch (error) {
//       console.error('Matching error: ', error);
//       out[outputKey] = 'Error matching string: ' + string + error;
//     }
//     return out;
//   };


//   let dataset = [
//     {'000': 'abc123'},
//     {'106': 'def456'},
//     {'7777': 'ghabc789'},
//     {'424': 'xyzabc'},
//   ];

//   let r1 = (key, values) => {
//     let out = {};


//     let uniqueValues = new Set(values);


//     out[key] = Array.from(uniqueValues);

//     return out;
//   };


//   const doMapReduce = (cb) => {
//     distribution.dlib.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }

//       distribution.dlib.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
//         try {
//           const regex = new RegExp('abc');
//           expect(v.length).toBe(dataset.filter((o) =>
//             regex.test(Object.values(o)[0])).length);
//           console.log('e, v for string matching: ', e, v);
//           done();
//         } catch (e) {
//           done(e);
//         }
//       });
//     });
//   };

//   let cntr = 0;

//   // Send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.dlib.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
//   // console.log('standalone1:::', m1('000','abc123'))
//   // console.log('standalone2:::', m1('106','def456'))
//   // done();
// });

// test('(25 pts) Inverted index wordflow', (done) => {
//   let m1 = (key, content) => {
//     const terms = content.match(/\w+/g) || [];
//     let out = [];
//     terms.forEach((term) => {
//       let termKey = term.toLowerCase();
//       let mapping = {};
//       mapping[termKey] = [key];
//       out.push(mapping);
//     });
//     return out;
//   };

//   let r1 = (term, listOfDocIdArrays) => {
//     let out = {};
//     let docIds = [...new Set(listOfDocIdArrays.flat())];
//     out[term] = docIds;
//     return out;
//   };

//   // New dataset for string matching
//   let dataset = [
//     {'id': 'doc1', 'content': 'The quick brown fox jumps over the lazy dog'},
//     {'id': 'doc2', 'content':
//       'A quick movement of the enemy will jeopardize six gunboats'},
//     {'id': 'doc3', 'content':
//       'All questions asked by five watched experts amaze the judge'},
//     {'id': 'doc4', 'content': 'The five boxing wizards jump quickly'},
//   ];
//   function generateExpectedOutput(dataset) {
//     const invertedIndex = {};

//     dataset.forEach((document) => {
//       const terms = document.content.match(/\w+/g) || [];
//       terms.forEach((term) => {
//         const lowerCaseTerm = term.toLowerCase();
//         if (!invertedIndex[lowerCaseTerm]) {
//           invertedIndex[lowerCaseTerm] = new Set();
//         }
//         invertedIndex[lowerCaseTerm].add(document.id);
//       });
//     });

//     for (const term in invertedIndex) {
//       if (invertedIndex.hasOwnProperty(term)) {
//         invertedIndex[term] =
//           Array.from(invertedIndex[term]);
//       }
//     }
//     return invertedIndex;
//   }
//   const expectedOutput = generateExpectedOutput(dataset);
//   function verifyMapReduceOutput(actualOutput, expectedOutput) {
//     let actualOutputObj = actualOutput.reduce((acc, curr) => {
//       const [key, value] = Object.entries(curr)[0];
//       acc[key] = value;
//       return acc;
//     }, {});


//     for (let term in expectedOutput) {
//       if (Object.prototype.hasOwnProperty.call(expectedOutput, term)) {
//         if (!actualOutputObj.hasOwnProperty(term)) {
//           console.error(`Missing term in actual output: ${term}`);
//           return false;
//         }


//         let expectedDocs = expectedOutput[term].sort();
//         let actualDocs = actualOutputObj[term].sort();

//         if (expectedDocs.length !== actualDocs.length ||
//             !expectedDocs.every((val, index) => val === actualDocs[index])) {
//           console.error(`Mismatch for term '${term}': expected 
//             ${expectedDocs.join(', ')} but got ${actualDocs.join(', ')}`);
//           return false;
//         }
//       }
//     }


//     for (let term in actualOutputObj) {
//       if (!expectedOutput.hasOwnProperty(term)) {
//         console.error(`Extra term in actual output: ${term}`);
//         return false;
//       }
//     }

//     console.log('Map-reduce output is correct.');
//     return true;
//   }

//   // Adjusted logic for map-reduce
//   const doMapReduce = (cb) => {
//     distribution.invertedIdx.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }

//       distribution.invertedIdx.mr
//           .exec({keys: v, map: m1, reduce: r1}, (e, v) => {
//             try {
//               console.log('e, v for string matching: ', e, v);
//               console.log('expected output: ', expectedOutput);
//               expect(verifyMapReduceOutput(v, expectedOutput)).toBeTruthy();
//               done();
//             } catch (e) {
//               done(e);
//             }
//           });
//     });
//   };

//   let cntr = 0;

//   // Send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = o.id;
//     let value = o.content;
//     distribution.invertedIdx.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
// });

// test('(25 pts) Reverse web link graph wordflow', (done) => {
//   let m1 = (source, sink) => {
//     const out = {};
//     out[sink] = [source];
//     return out;
//   };

//   let r1 = (sink, sources) => {
//     let out = {};
//     // Flatten the array of source arrays and remove duplicates
//     let uniqueSources = [...new Set(sources.flat())];
//     out[sink] = uniqueSources;
//     return out;
//   };

//   let dataset = [
//     {source: 'siteA.com/home', sink: 'siteB.com/about'},
//     {source: 'siteC.com/info', sink: 'siteB.com/about'},
//     {source: 'siteD.com/contact', sink: 'siteA.com/home'},
//     {source: 'siteE.com/blog', sink: 'siteA.com/home'},
//     {source: 'siteF.com', sink: 'siteG.com/promo'},
//     {source: 'siteH.com', sink: 'siteI.com'},
//     {source: 'siteJ.com', sink: 'siteK.com/landing'},
//     {source: 'siteL.com', sink: 'siteM.com'},
//     {source: 'siteN.com', sink: 'siteO.com/features'},
//     {source: 'siteP.com', sink: 'siteQ.com'},
//     {source: 'siteR.com', sink: 'siteS.com'},
//     {source: 'siteT.com', sink: 'siteU.com'},
//     {source: 'siteV.com', sink: 'siteW.com'},
//     {source: 'siteX.com', sink: 'siteY.com'},
//   ];
//   function generateExpectedOutput(dataset) {
//     const reverseLinkGraph = {};

//     dataset.forEach((link) => {
//       if (!reverseLinkGraph[link.sink]) {
//         reverseLinkGraph[link.sink] = [];
//       }
//       reverseLinkGraph[link.sink].push(link.source);
//     });

//     for (const sink in reverseLinkGraph) {
//       if (reverseLinkGraph.hasOwnProperty(sink)) {
//         reverseLinkGraph[sink].sort();
//       }
//     }

//     return reverseLinkGraph;
//   }
//   const expectedOutput = generateExpectedOutput(dataset);
//   function verifyMapReduceOutput(actualOutput, expectedOutput) {
//     let actualOutputObj = actualOutput.reduce((acc, curr) => {
//       const [key, value] = Object.entries(curr)[0];
//       acc[key] = value;
//       return acc;
//     }, {});

//     for (let sink in expectedOutput) {
//       if (Object.prototype.hasOwnProperty.call(expectedOutput, sink)) {
//         if (!actualOutputObj.hasOwnProperty(sink)) {
//           console.error(`Missing sink in actual output: ${sink}`);
//           return false;
//         }

//         let expectedSources = expectedOutput[sink].sort();
//         let actualSources = actualOutputObj[sink].sort();

//         if (expectedSources.length !== actualSources.length ||
//             !expectedSources.
//                 every((val, index) => val === actualSources[index])) {
//           console.error(`Mismatch for sink '${sink}': 
//             expected ${expectedSources.join(', ')} 
//               but got ${actualSources.join(', ')}`);
//           return false;
//         }
//       }
//     }

//     for (let sink in actualOutputObj) {
//       if (!expectedOutput.hasOwnProperty(sink)) {
//         console.error(`Extra sink in actual output: ${sink}`);
//         return false;
//       }
//     }

//     console.log('Map-reduce output for reverse web link graph is correct.');
//     return true;
//   }

//   const doMapReduce = (cb) => {
//     distribution.sourceSink.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }

//       distribution.sourceSink.mr
//           .exec({keys: v, map: m1, reduce: r1}, (e, v) => {
//             try {
//               console.log('e, v for reverse web link graph: ', e, v);
//               console.log('expected output: ', expectedOutput);
//               expect(verifyMapReduceOutput(v, expectedOutput)).toBeTruthy();
//               done();
//             } catch (e) {
//               done(e);
//             }
//           });
//     });
//   };

//   let cntr = 0;

//   dataset.forEach((o) => {
//     let key = o.source;
//     let value = o.sink;
//     distribution.sourceSink.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });

//   // console.log('standalone1:::', m1('siteA.com/home','siteB.com/about'))
//   // console.log('standalone2:::', m1('106','def456'))
//   // done();
// });

// test('(25 pts) test1', (done) => {
//   let m1 = (key, string) => {
//     const regex = new RegExp('789');
//     let out = {};
//     const outputKey = global.distribution.util.id.getID(string);
//     try {
//       if (regex.test(string)) {
//         out[outputKey] = key;
//       }
//     } catch (error) {
//       console.error('Matching error: ', error);
//       out[outputKey] = 'Error matching string: ' + string + error;
//     }
//     return out;
//   };

//   let dataset = [
//     {'000': 'abc123'},
//     {'106': 'def456'},
//     {'7777': 'ghabc789'},
//     {'424': 'xyzabc'},
//   ];

//   let r1 = (key, values) => {
//     let out = {};

//     let uniqueValues = new Set(values);

//     out[key] = Array.from(uniqueValues);

//     return out;
//   };

//   // Adjusted logic for map-reduce
//   const doMapReduce = (cb) => {
//     distribution.test1.store.get(null, (e, v) => {
//       try {
//         expect(v.length).toBe(dataset.length);
//       } catch (e) {
//         done(e);
//       }

//       distribution.test1.mr.exec({keys: v, map: m1, reduce: r1}, (e, v) => {
//         try {
//           const regex = new RegExp('789');
//           expect(v.length).toBe(dataset
//               .filter((o) => regex.test(Object.values(o)[0])).length);
//           console.log('e, v for string matching: ', e, v);
//           done();
//         } catch (e) {
//           done(e);
//         }
//       });
//     });
//   };

//   let cntr = 0;

//   // Send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.test1.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
//   // console.log('standalone1:::', m1('000','abc123'))
//   // console.log('standalone2:::', m1('106','def456'))
//   // done();
// });

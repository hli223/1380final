const startPort = 8000;
global.nodeConfig = { ip: '127.0.0.1', port: startPort };
const { url } = require('inspector');
const distribution = require('../distribution');
const id = distribution.util.id;

// const ipAddresses = ['18.225.175.3', '3.142.135.227']
const ipAddresses = ['127.0.0.1', '127.0.0.1']

const groupsTemplate = require('../distribution/all/groups');


const crawlUrlGroup = {};
const downloadTextGroup = {};
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

const nodes = [];
for (let i = 1; i <= 5; i++) {
    nodes.push({ ip: '127.0.0.1', port: startPort + i });
}


beforeAll((done) => {
    /* Stop the nodes if they are running */

    nodes.forEach(node => {
        crawlUrlGroup[id.getSID(node)] = node;
        downloadTextGroup[id.getSID(node)] = node;
        invertedIdxGroup[id.getSID(node)] = node;
        sourceSinkGroup[id.getSID(node)] = node;
        test1Group[id.getSID(node)] = node;
    });

    let cntr = 0;
    const startNodes = (cb) => {
        nodes.forEach(node => {
            distribution.local.status.spawn(node, (e, v) => {
                // Handle the callback
                cntr++;
                if (cntr === nodes.length) {
                    console.log('all nodes started!');
                    cb();
                }
            });
        });
    };

    distribution.node.start((server) => {
        localServer = server;

        const crawlUrlConfig = { gid: 'crawlUrl' };
        startNodes(() => {
            groupsTemplate(crawlUrlConfig).put(crawlUrlConfig, crawlUrlGroup, (e, v) => {
                const downloadTextConfig = { gid: 'downloadText' };
                groupsTemplate(downloadTextConfig).put(downloadTextConfig, downloadTextGroup, (e, v) => {
                    const invertedIdxConfig = { gid: 'invertedIdx' };
                    groupsTemplate(invertedIdxConfig).
                        put(invertedIdxConfig, invertedIdxGroup, (e, v) => {
                            const sourceSinkConfig = { gid: 'sourceSink' };
                            groupsTemplate(sourceSinkConfig).
                                put(sourceSinkConfig, sourceSinkGroup, (e, v) => {
                                    const test1Config = { gid: 'test1' };
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
}, 400000);


// shut down the nodes
afterAll((done) => {
    let cntr = 0;
    const remote = { service: 'status', method: 'stop' };
    nodes.forEach(node => {
        remote.node = node;
        distribution.local.comm.send([], remote, (e, v) => {
            // Handle the callback
            cntr++;
            if (cntr === nodes.length) {
                console.log('all nodes stopped!');
                localServer.close();
                done();
            }
        });
    });
});


test('(25 pts) Inverted index wordflow', (done) => {
    // the input should be in form of {key: content}
    // the output should be in form of {term: [{key1: cnt1}, {key2: cnt2}, ...]}
    let m1 = (key, inputObj) => {
        // key: string, the url of the document
        // content: string, the content of the document
        // output: array of objects, each object has a single key-value pair
        console.log('input obj is ', inputObj);
        let url = inputObj.url;
        let content = inputObj.htmlContent;
        console.log('map input is ', url, content);
        console.log('content type', typeof content);
        let terms = content.match(/\w+/g) || [];
        // stem each term
        terms = terms.map((term) => global.stemmer.stem(term));


        console.log('stemmer result is: ', terms);
        let out = {};
        //{word: url}
        //{word:[url]}
        terms.forEach((term) => {
            let termKey
            if (term !== 'set') {
                termKey = term.toLowerCase();
            } else {
                termKey = 'Set';//added due to some weird deserialize/serialize issue
            }
            // let termKey = global.stemmer.stem(term.toLowerCase());
            // let mapping = {};
            // mapping[termKey] = url;
            if (!out[termKey]) {
                out[termKey] = [url];
            } else {
                out[termKey].push(url);
            }
            // out.push(mapping);
        });
        return out;
    };

    let r1 = (term, listOfDocIdArray) => {
        //listOfDocIdArray = [url1, url2, ...]
        // term: string, the term to be reduced
        // listOfDocIdArray: an array containing the doc ids that contain the term
        // output: {term: [{docId1: cnt1}, {docId2: cnt2}, ...]}
        console.log('the input of reduce function', term, listOfDocIdArray);
        let out = {};
        // first count the number of occurrences of the docId in the listOfDocIdArray
        // the result should be in the form of {docId: count}
        let docIds = {};
        listOfDocIdArray.forEach((docId) => {
            if (!docIds[docId]) {
                docIds[docId] = 0;
            }
            docIds[docId]++;
        });
        console.log('the result of counting', docIds);
        // then sort the docIds by the number of occurrences in descending order
        docIds = Object.entries(docIds).map((v) => {
            let mapping = {};
            mapping[v[0]] = v[1];
            return mapping;
        });
        console.log('the result of format convert:', docIds);
        out[term] = docIds;

        //out = {term1:[{actualUrl1: count}, {actualUrl2: count}, ...]} sort by number of occurences in descending order
        console.log('the result of reduce function', out);
        return out;
    };

    const invertIndex = async (cb) => {
        let urlKeys;
        try {
            urlKeys = await global.promisify(distribution.downloadText.store.get)(null);
            // urlKeys = urlKeys.slice(0, 6);
            console.log('Retrieved all url keys, number of keys: ', urlKeys.length);
        } catch (e) {
            console.error('Error fetching urlKeys', e);
            done(e);
        }

        let configuration = { map: m1, reduce: r1, storeGroup: 'invertedIdx' };

        let execMr = global.promisify(distribution.downloadText.mr.exec)
        let batchSize =5;
        let totalNumKeys = urlKeys.length;//urlKeys.length
        for (let i = 0; i < totalNumKeys; i += batchSize) {
            if (i + batchSize > totalNumKeys) {
                batchSize = totalNumKeys - i;
            }
            let batch = urlKeys.slice(i, i + batchSize);
            console.log('batch: ', batch, i, i + batchSize, totalNumKeys);
            configuration.keys = batch;
            try {
                await execMr(configuration);
            } catch (err) {
                console.error('invert index errorr: ', err.stack);
                done(err);
            }
        }
        if (totalNumKeys % batchSize !== 0) {
            let lastBatch = urlKeys.slice(-totalNumKeys % batchSize);
            console.log('lastBatch: ', lastBatch, totalNumKeys % batchSize);
            configuration.keys = lastBatch;
            try {
                await execMr(configuration);
            } catch (err) {
                console.error('invert index errorr: ', err.stack);
                done(err);
            }
        }
    };

    invertIndex().then(() => {
        done();
    }).catch(err => {
        console.error('Error in invert index: ', err);
        done(err);
    });

    // New dataset for string matching
    // let dataset = [];

    // function generateExpectedOutput(dataset) {
    //     const invertedIndex = {};

    //     dataset.forEach((document) => {
    //         const terms = document.content.match(/\w+/g) || [];
    //         terms.forEach((term) => {
    //             const lowerCaseTerm = global.stemmer.stem(term.toLowerCase());
    //             // const lowerCaseTerm = term.toLowerCase();
    //             if (!invertedIndex[lowerCaseTerm]) {
    //                 invertedIndex[lowerCaseTerm] = [];
    //             }
    //             invertedIndex[lowerCaseTerm].push(document.id);
    //         });
    //     });

    //     for (const term in invertedIndex) {
    //         // count the occurrences of each document
    //         const docCount = {};
    //         invertedIndex[term].forEach((docId) => {
    //             if (!docCount[docId]) {
    //                 docCount[docId] = 0;
    //             }
    //             docCount[docId]++;
    //         });
    //         // create an array of objects with the document id and the occurrence count
    //         // and sort it by the count in descending order
    //         invertedIndex[term] = Object.entries(docCount)
    //             .sort((a, b) => b[1] - a[1])
    //             .map(([docId, count]) => ({ [docId]: count }));
    //     }
    //     return invertedIndex;
    // }
    // // const expectedOutput = generateExpectedOutput(dataset);
    // function verifyMapReduceOutput(actualOutput, expectedOutput) {
    //     let actualOutputObj = actualOutput.reduce((acc, curr) => {
    //         const [key, value] = Object.entries(curr)[0];
    //         acc[key] = value;
    //         return acc;
    //     }, {});


    //     for (let term in expectedOutput) {
    //         if (Object.prototype.hasOwnProperty.call(expectedOutput, term)) {
    //             if (!actualOutputObj.hasOwnProperty(term)) {
    //                 console.error(`Missing term in actual output: ${term}`);
    //                 return false;
    //             }


    //             let expectedDocs = expectedOutput[term].sort();
    //             let actualDocs = actualOutputObj[term].sort();
    //             let expectedDocsObj = {};
    //             let actualDocsObj = {};
    //             // convert the expectedDocs array to an object for easier comparison
    //             expectedDocs.forEach((doc) => {
    //                 const [docId, count] = Object.entries(doc)[0];
    //                 expectedDocsObj[docId] = count;
    //             });
    //             // convert the actualDocs array to an object for easier comparison
    //             actualDocs.forEach((doc) => {
    //                 const [docId, count] = Object.entries(doc)[0];
    //                 actualDocsObj[docId] = count;
    //             });
    //             console.log('expectedDocs: ', term, expectedDocsObj);
    //             console.log('actualDocs: ', term, actualDocsObj);
    //             if (expectedDocs.length !== actualDocs.length) {
    //                 console.error(`Mismatch for term '${term}': expected 
    //           ${expectedDocs.join(', ')} but got ${actualDocs.join(', ')}`);
    //                 return false;
    //             }
    //             // loop through all the expected documents and compare the counts
    //             for (let docId in expectedDocsObj) {
    //                 if (!actualDocsObj.hasOwnProperty(docId)) {
    //                     console.error(`Missing document ${docId} for term ${term}`);
    //                     return false;
    //                 }
    //                 if (expectedDocsObj[docId] !== actualDocsObj[docId]) {
    //                     console.error(`Mismatch for term '${term}': expected
    //                  ${expectedDocs.join(', ')} but got ${actualDocs.join(', ')}`);
    //                     return false;
    //                 }
    //             }
    //         }
    //     }


    //     for (let term in actualOutputObj) {
    //         if (!expectedOutput.hasOwnProperty(term)) {
    //             console.error(`Extra term in actual output: ${term}`);
    //             return false;
    //         }
    //     }

    //     console.log('Map-reduce output is correct.');
    //     return true;
    // }

    // Adjusted logic for map-reduce
    // const doMapReduce = (cb) => {
    //     distribution.downloadText.store.get(null, (e, contentKeys) => {
    //         console.log('get downloaded content: ', e, contentKeys);

    //         distribution.downloadText.mr
    //             .exec({ keys: contentKeys, map: m1, reduce: r1, storeGroup: "invertedIdx" }, (e, v) => {
    //                 try {
    //                     console.log('e, v for string matching: ', e, v);
    //                     // console.log('expected output: ', expectedOutput);
    //                     // expect(verifyMapReduceOutput(v, expectedOutput)).toBeTruthy();
    //                     done();
    //                 } catch (e) {
    //                     done(e);
    //                 }
    //             });
    //     });
    // };

    // let cntr = 0;

    // // Send the dataset to the cluster
    // // dataset.forEach((o) => {
    // //     let key = o.id;
    // //     let value = o.content;
    // //     distribution.invertedIdx.store.put(value, key, (e, v) => {
    // //         cntr++;
    // //         // Once we are done, run the map reduce
    // //         if (cntr === dataset.length) {
    // //             doMapReduce();
    // //         }
    // //     });
    // // });
    // doMapReduce();
}, 5000000);


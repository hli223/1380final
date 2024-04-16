global.nodeConfig = { ip: '127.0.0.1', port: 7070 };
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');

const invertedIdxGroup = {};

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

const n1 = { ip: '127.0.0.1', port: 7110 };
const n2 = { ip: '127.0.0.1', port: 7111 };
const n3 = { ip: '127.0.0.1', port: 7112 };

beforeAll((done) => {
    /* Stop the nodes if they are running */

    invertedIdxGroup[id.getSID(n1)] = n1;
    invertedIdxGroup[id.getSID(n2)] = n2;
    invertedIdxGroup[id.getSID(n3)] = n3;


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

        const invertedIdxConfig = { gid: 'invertedIdx' };
        startNodes(() => {
            groupsTemplate(invertedIdxConfig).put(invertedIdxConfig, invertedIdxGroup, (e, v) => {
                done();
            });
        });
    });
});


// shut down the nodes
afterAll((done) => {
    let remote = { service: 'status', method: 'stop' };
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


test('(25 pts) Inverted index wordflow', (done) => {
    // the input should be in form of {key: content}
    // the output should be in form of {term: [{key1: cnt1}, {key2: cnt2}, ...]}
    let m1 = (key, content) => {
        // key: string, the url of the document
        // content: string, the content of the document
        // output: array of objects, each object has a single key-value pair
        const terms = content.match(/\w+/g) || [];
        // stem each term
        // terms = terms.map((term) => stemmer(term));

        let out = [];
        terms.forEach((term) => {
            let termKey = term.toLowerCase();
            let mapping = {};
            mapping[termKey] = key;
            out.push(mapping);
        });
        console.log('the result of map function', out);
        return out;
    };

    let r1 = (term, listOfDocIdArray) => {
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
        docIds = Object.entries(docIds).sort((a, b) => b[1] - a[1]).map((v) => {
            let mapping = {};
            mapping[v[0]] = v[1];
            return mapping;
        });
        out[term] = docIds;
        console.log('the result of reduce function', out);
        return out;
    };

    // New dataset for string matching
    let dataset = [
        { 'id': 'doc1', 'content': 'The quick brown fox jumps over the lazy dog' },
        {
            'id': 'doc2', 'content':
                'A quick movement of the enemy will jeopardize six gunboats'
        },
        {
            'id': 'doc3', 'content':
                'All questions asked by five watched experts amaze the judge'
        },
        { 'id': 'doc4', 'content': 'The five boxing wizards jump quickly' },
    ];
    function generateExpectedOutput(dataset) {
        const invertedIndex = {};

        dataset.forEach((document) => {
            const terms = document.content.match(/\w+/g) || [];
            terms.forEach((term) => {
                const lowerCaseTerm = term.toLowerCase();
                if (!invertedIndex[lowerCaseTerm]) {
                    invertedIndex[lowerCaseTerm] = [];
                }
                invertedIndex[lowerCaseTerm].push(document.id);
            });
        });

        for (const term in invertedIndex) {
            // count the occurrences of each document
            const docCount = {};
            invertedIndex[term].forEach((docId) => {
                if (!docCount[docId]) {
                    docCount[docId] = 0;
                }
                docCount[docId]++;
            });
            // create an array of objects with the document id and the occurrence count
            // and sort it by the count in descending order
            invertedIndex[term] = Object.entries(docCount)
                .sort((a, b) => b[1] - a[1])
                .map(([docId, count]) => ({ [docId]: count }));
        }
        return invertedIndex;
    }
    const expectedOutput = generateExpectedOutput(dataset);
    function verifyMapReduceOutput(actualOutput, expectedOutput) {
        let actualOutputObj = actualOutput.reduce((acc, curr) => {
            const [key, value] = Object.entries(curr)[0];
            acc[key] = value;
            return acc;
        }, {});


        for (let term in expectedOutput) {
            if (Object.prototype.hasOwnProperty.call(expectedOutput, term)) {
                if (!actualOutputObj.hasOwnProperty(term)) {
                    console.error(`Missing term in actual output: ${term}`);
                    return false;
                }


                let expectedDocs = expectedOutput[term].sort();
                let actualDocs = actualOutputObj[term].sort();
                let expectedDocsObj = {};
                let actualDocsObj = {};
                // convert the expectedDocs array to an object for easier comparison
                expectedDocs.forEach((doc) => {
                    const [docId, count] = Object.entries(doc)[0];
                    expectedDocsObj[docId] = count;
                });
                // convert the actualDocs array to an object for easier comparison
                actualDocs.forEach((doc) => {
                    const [docId, count] = Object.entries(doc)[0];
                    actualDocsObj[docId] = count;
                });
                console.log('expectedDocs: ', expectedDocsObj);
                console.log('actualDocs: ', actualDocsObj);
                if (expectedDocs.length !== actualDocs.length) {
                    console.error(`Mismatch for term '${term}': expected 
              ${expectedDocs.join(', ')} but got ${actualDocs.join(', ')}`);
                    return false;
                }
                // loop through all the expected documents and compare the counts
                for (let docId in expectedDocsObj) {
                    if (!actualDocsObj.hasOwnProperty(docId)) {
                        console.error(`Missing document ${docId} for term ${term}`);
                        return false;
                    }
                    if (expectedDocsObj[docId] !== actualDocsObj[docId]) {
                        console.error(`Mismatch for term '${term}': expected
                     ${expectedDocs.join(', ')} but got ${actualDocs.join(', ')}`);
                        return false;
                    }
                }
            }
        }


        for (let term in actualOutputObj) {
            if (!expectedOutput.hasOwnProperty(term)) {
                console.error(`Extra term in actual output: ${term}`);
                return false;
            }
        }

        console.log('Map-reduce output is correct.');
        return true;
    }

    // Adjusted logic for map-reduce
    const doMapReduce = (cb) => {
        distribution.invertedIdx.store.get(null, (e, v) => {
            try {
                expect(v.length).toBe(dataset.length);
            } catch (e) {
                done(e);
            }

            distribution.invertedIdx.mr
                .exec({ keys: v, map: m1, reduce: r1 }, (e, v) => {
                    try {
                        console.log('e, v for string matching: ', e, v);
                        console.log('expected output: ', expectedOutput);
                        expect(verifyMapReduceOutput(v, expectedOutput)).toBeTruthy();
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
        });
    };

    let cntr = 0;

    // Send the dataset to the cluster
    dataset.forEach((o) => {
        let key = o.id;
        let value = o.content;
        distribution.invertedIdx.store.put(value, key, (e, v) => {
            cntr++;
            // Once we are done, run the map reduce
            if (cntr === dataset.length) {
                doMapReduce();
            }
        });
    });
});
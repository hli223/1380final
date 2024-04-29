const startPort = 8000;
global.nodeConfig = { ip: '127.0.0.1', port: startPort };
const distribution = require('../distribution');
const id = distribution.util.id;

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
for (let i = 1; i <= 6; i++) {
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
}, 40000);


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



test('(25 pts) Query wordflow', (done) => {

}, 800000);


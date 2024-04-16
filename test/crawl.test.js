global.nodeConfig = {ip: '127.0.0.1', port: 7070};
const distribution = require('../distribution');
const id = distribution.util.id;

const groupsTemplate = require('../distribution/all/groups');



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
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      var htmlContent = await response.text();
      htmlContent = htmlContent.replace("\u00a9", "&copy;")
    let urls = [];
    const dom = new global.JSDOM(htmlContent);
    const document = dom.window.document;


    const anchors = document.querySelectorAll('a');

    anchors.forEach((anchor) => {
        const href = anchor.getAttribute('href');
        if (href) {
        const absoluteUrl = new URL(href, url).toString();
        urls.push({ url: absoluteUrl, depth: depth + 1, parent: url });
        }
    });

      out[url] = urls;
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
    // {'424': 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/level_2a/'},
    {key: '421', url: 'https://cs.brown.edu/courses/csci1380/sandbox/1'},
    // {'243': 'https://atlas.cs.brown.edu/data/dblp/'}
  ];

    const baseUrl = dataset[0].url;
    const doMapReduce = (cb) => {
    
    const visited = new Set();
    // const queue = [{ url: dataset[0].url, depth: 0, parent: null}];
    var currDepth = 0;
    const levels = [[dataset[0].url]];

    console.log('start running')

    // while (levels[currDepth].length > 0) {

        const urlKeys = [];
        


        function levelCrawl(urlKeys) {
            console.log('start level crawl, level: ', currDepth, urlKeys);
            if (urlKeys===undefined) {
                done();
            }

            distribution.ncdc.mr.exec({keys: urlKeys, map: m1, reduce: null, notStore: true}, (e, v) => {
                try {
                    console.log('mapreduce result: ', v);
                    currDepth++;
                    levels[currDepth] = v;
                } catch (e) {
                    done(e);
                }
            });
        }


        let urlsToBeStore = []
        levels[currDepth].forEach((url) => {
            if ((visited.has(url) || url.length < baseUrl.length && baseUrl.includes(url))) {
                completedUrls++;
                if (completedUrls === levels[currDepth].length) {
                    levelCrawl();
                }
            } else {
                visited.add(url);
                console.log(url);
                const urlKey = id.getID(url);
                urlKeys.push(urlKey);
                urlsToBeStore.push({url: url, key: urlKey});
            }

        });

        console.log('urls to be store: ', urlsToBeStore);

        let completedUrls = 0;

        const checkAllDoneUrlsStore = () => {
            completedUrls++;
            if (completedUrls === urlsToBeStore.length) {
                // levelCrawl(urlKeys);
                console.log('store completed!')
            }
        }

        for (let i = 0; i < urlsToBeStore.length; i++) {
            let url = urlsToBeStore[i].url;
            let urlKey = urlsToBeStore[i].key;
            console.log('start storing!', url, urlKey)
            global.distribution['ncdc'].store.put(url, urlKey, (e, v) => {
                checkAllDoneUrlsStore();
            });
        }


        // urlsToBeStore.forEach((urlInfo) => {
        //     let url = urlInfo.url;
        //     let urlKey = urlInfo.key;
        //     console.log('start storing!', url, urlKey)
        //     global.distribution['ncdc'].store.put(url, urlKey, (e, v) => {
        //         completedUrls++;
        //         if (completedUrls === urlsToBeStore.length) {
        //             // levelCrawl(urlKeys);
        //             console.log('store completed!')
        //         }
        //     });
        // });

        // distribution.ncdc.store.put(url, urlKey, (e, v) => {
        //     completedUrls++;
        //     console.log('urlKeys in store: ', urlKeys);
        //     if (completedUrls === levels[currDepth].length) {
        //         // levelCrawl(urlKeys);
        //         console.log('store completed!')
        //     }
        // });


    // }

    done();
  };
    doMapReduce();

//   let cntr = 0;

//   // We send the dataset to the cluster
//   dataset.forEach((o) => {
//     let key = Object.keys(o)[0];
//     let value = o[key];
//     distribution.ncdc.store.put(value, key, (e, v) => {
//       cntr++;
//       // Once we are done, run the map reduce
//       if (cntr === dataset.length) {
//         doMapReduce();
//       }
//     });
//   });
//   m1('000', dataset[1]['106']).then((res) => {
//     console.log(res);
//     done();
//   });
});
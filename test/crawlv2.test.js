const startPort = 8000;
global.nodeConfig = { ip: '127.0.0.1', port: startPort };
const distribution = require('../distribution');
const id = distribution.util.id;
const fs = require('fs');

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
}, 400000);

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

test('(25 pts) crawler workflow', (done) => {
  let m1 = async (key, baseUrl) => {
    await new Promise(resolve => setTimeout(resolve, 2000));
    if (baseUrl === undefined) {
      return {};
    }
    try {
      if (baseUrl.slice(-1) !== '/' && !baseUrl.endsWith('.txt') && !baseUrl.endsWith('.html')) {
        baseUrl += '/'
      }
    } catch (e) {
      console.log('error in m1: ' + key + ' ' + baseUrl + ' ', e);
      return { url: ['error in m1: ' + key + ' ' + baseUrl + ' ', e] };
    }

    let out = {};
    try {
      process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
      const response = await global.fetch(baseUrl);
      if (!response.ok) {
        // throw new Error(`HTTP error! status: ${response.status}`);
        return { ...out, [baseUrl]: `HTTP error! status: ${response.status}` };
      }
      var htmlContent = await response.text();
      htmlContent = htmlContent.replace("\u00a9", "&copy;")
      let urls = [];
      const dom = new global.JSDOM(htmlContent);
      const document = dom.window.document;

      console.log('baseUrl in map is: ', baseUrl);
      const anchors = document.querySelectorAll('a');

      anchors.forEach((anchor) => {
        const href = anchor.getAttribute('href');
        if (href) {
          var absoluteUrl = new URL(href, baseUrl).toString();
          // if (absoluteUrl.endsWith('/')) {
          //   absoluteUrl = absoluteUrl.slice(0, -1);
          // }
          if (absoluteUrl.endsWith('index.html')) {
            absoluteUrl = new URL(absoluteUrl + '/../').toString();
          }
          if (!baseUrl.startsWith(absoluteUrl)) {
            urls.push(absoluteUrl);
          }


        }
      });
      out[baseUrl] = urls;
    } catch (error) {
      console.error(baseUrl + ' Fetch error: ', error);
      out = { ...out, [url]: 'Error fetching URL: ' + baseUrl + ' ' + error };
      // out = {}
    }
    return out;
  };





  var currDepth = 0;
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/books.txt'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg';
  // baseUrl = 'https://www.gutenberg.org/ebooks/'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/?C=N;O=D'//problemetic
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/?C=D;O=A'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/8/12380/?C=M;O=A'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/1/2/3/7/?C=N;O=D/'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/mesaerion-the-best-science-fiction-stories-1800-1849_983/index.html';//problemetic
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/category/books/default_15/';
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/the-book-of-mormon_571/index.html'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/the-book-of-mormon_571/'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/4/tag/truth/index.html';
  var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3'
  // var baseUrl = 'https://www.usenix.org/publications/proceedings'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/category/books/science-fiction_16'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/4/tag/authors/page/1'
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/2/static/book1.txt'//text cannot be downloaded
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/level_2a'//this one is downloadable
  // var baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/category/books_1'
  // baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/1/level_1a/'

  // const visited = new Set();
  if (baseUrl.endsWith('/')) {
    baseUrl = baseUrl.slice(0, -1);
  }
  if (baseUrl.endsWith('index.html')) {
    baseUrl = new URL(baseUrl + '/../').toString();
  }
  const levels = [[baseUrl]];
  console.log('baseURL is ', baseUrl);
  function crawl() {
    const levelCrawl = (urlKeys) => {
      // console.log('start level crawl, level: ', currDepth, urlKeys);
      if (urlKeys === undefined || urlKeys.length === 0) {
        done();
      }
      distribution.crawlUrl.mr.exec({ keys: urlKeys, map: m1, reduce: null, notStore: true, returnMapResult: true }, (e, v) => {
        //v is a set
        if (e !== null && Object.keys(e).length > 0) {
          console.log('map reduce errorr: ', e);
          done(e);
          return;
        }
        try {
          // console.log('mapreduce result at level: ', currDepth, v, urlKeys);
          currDepth++;
          const newUrls = []
          // for (let i = 0; i < Object.keys(v).length; i++) {
          //   if (v[Object.keys(v)[i]].length > 0) {
          //     newUrls.push(...v[Object.keys(v)[i]][0]);
          //   }
          // }
          console.log('map reduce done!', v)
          let cntr = 0;
          v.forEach((mapKey) => {
            distribution.crawlUrl.store.get(mapKey, (e, value) => {
              
              if (e) {
                done(e);
              }
              newUrls.push(...value);
              cntr++;
              console.log('after map reduce, store.get value: ', value, newUrls)
              if (cntr === v.size) {
                levels.push(newUrls);
                console.log('start crawl() again')
                crawl();
              }
            });
          });
        } catch (e) {
          console.log('error in levelcrawl: ', e);
          done(e);
        }
      });
    }
    // if (levels[currDepth].length === 0) {
    //   console.log('allUrls: ', visited.size);
    //   done();
    // }
    console.log('level[currDepth]: ', levels[currDepth], currDepth);
    let urlsToBeStore = [];
    const urlKeys = [];
    const keyUrlsMap = {}
    levels[currDepth].forEach((url) => {
      if (url.endsWith('/')) {
        url = url.slice(0, -1);
      }
      // if (url.length > 0 && !(visited.has(url) || url.length < baseUrl.length && baseUrl.includes(url)) && url.includes(baseUrl)) {
      //   visited.add(url);
        const urlKey = 'url-' + id.getID(url);
        urlKeys.push(urlKey);
        keyUrlsMap[urlKey] = url;
        urlsToBeStore.push({ url: url, key: urlKey });
      // }
    });

    let cntr = 0;
    console.log('keyUrlsMap: ', keyUrlsMap)
    console.log('urlsToBeStore: ', urlsToBeStore, currDepth);
    if (urlsToBeStore.length === 0) {
      // console.log('allUrls: ', currDepth, visited.size, visited);
      // fs.writeFileSync('visited.txt', Array.from(visited).join('\n'));
      done();
      return;
    }
    urlsToBeStore.forEach((o) => {
      let key = o.key;
      let value = o.url;
      distribution.crawlUrl.store.put(value, key, (e, v) => {
        cntr++;
        console.log('put urlsToBeStore:', value, key, e, v)
        if (e) {
          done(e);
        }
        if (cntr === urlsToBeStore.length) {
          console.log('urlsToBeStore store done! check urlsToBeStore', currDepth, urlsToBeStore)
          levelCrawl(urlKeys);
          // doMapReduce();
        }
      });
    });

  }
  crawl();


}, 800000);

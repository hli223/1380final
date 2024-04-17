const startPort = 8000;
global.nodeConfig = {ip: '127.0.0.1', port: startPort};
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

const n1 = {ip: '127.0.0.1', port: startPort+1};
const n2 = {ip: '127.0.0.1', port: startPort+2};
const n3 = {ip: '127.0.0.1', port: startPort+3};


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
    if (url === undefined) {
      return {};
    }
    try {
      // // if (url.slice(-5)==='.html') {
      // //   url = url.slice(0, -10);
      // // } else if (url.slice(-1)!=='/' && !url.endsWith('.txt')) {
      // //   url = url + '/';
      // // }
      if (url.slice(-1)!=='/' && !url.endsWith('.txt')) {
        url += '/'
      }
    } catch (e) {
      console.log('error in m1: '+key+' '+url+ ' ', e);
      return {url:['error in m1: '+key+' '+url+ ' ', e]};
    }

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
            var absoluteUrl = new URL(href, url).toString();
            if (absoluteUrl.endsWith('.html')) {
              absoluteUrl = new URL(absoluteUrl+'/../').toString();
            } 
            urls.push(absoluteUrl);

          }
      });
      out[url] = urls;
    } catch (error) {
      console.error(url+' Fetch error: ', error);
      // out = {...out, [url]: 'Error fetching URL: '+url + ' ' + error};
      out = {}
    }
    return out;
  };

  


  
  var currDepth = 0;
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg/books.txt'
  // var baseUrl = 'https://atlas.cs.brown.edu/data/gutenberg';
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
  // baseUrl = 'https://cs.brown.edu/courses/csci1380/sandbox/3/catalogue/animal-farm_313/'


  const visited = new Set();
  if (baseUrl.endsWith('/')) {
    baseUrl = baseUrl.slice(0, -1);
  } 
  if (baseUrl.endsWith('.html')) {
    baseUrl = new URL(baseUrl+'/../').toString();
  } 
  const levels = [[baseUrl]];
  console.log('baseURL is ', baseUrl);
  function crawl() {
    const levelCrawl = (urlKeys) => {
        // console.log('start level crawl, level: ', currDepth, urlKeys);
        if (urlKeys===undefined || urlKeys.length===0) {
            done();
        }
        distribution.ncdc.mr.exec({keys: urlKeys, map: m1, reduce: null, notStore: true}, (e, v) => {
          if (e!==null && Object.keys(e).length > 0) {
            console.log('map reduce errorr: ', e);
            done(e);
            return;
          }
          try {
              // console.log('mapreduce result at level: ', currDepth, v, urlKeys);
              currDepth++;
              const newUrls = []
              for (let i = 0; i < Object.keys(v).length; i++) {
                if (v[Object.keys(v)[i]].length > 0) {
                  newUrls.push(...v[Object.keys(v)[i]][0]);
                }
              }
              levels.push(newUrls);
              crawl();
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
    console.log('level[currDepth]: ', levels[currDepth], currDepth, 'number of links:', visited.size);
    let urlsToBeStore = [];
    const urlKeys = [];
    const keyUrlsMap = {}
    levels[currDepth].forEach((url) => {
      if (url.length>0&&!(visited.has(url) || url.length < baseUrl.length && baseUrl.includes(url)) && url.includes(baseUrl)) {
        visited.add(url);
        console.log(url);
        const urlKey = 'url-'+id.getID(url);
        urlKeys.push(urlKey);
        keyUrlsMap[urlKey] = url;
        urlsToBeStore.push({url: url, key: urlKey});
      }
    });

    let cntr = 0;
    console.log('keyUrlsMap: ', keyUrlsMap)
    console.log('urlsToBeStore: ', urlsToBeStore, currDepth);
    if (urlsToBeStore.length === 0) {
      console.log('allUrls: ', currDepth, visited.size, visited);
      done();
      return;
    }
    urlsToBeStore.forEach((o) => {
      let key = o.key;
      let value = o.url;
      distribution.ncdc.store.put(value, key, (e, v) => {
        cntr++;
        console.log('put urlsToBeStore:', value, key, e, v)
        if (e) {
          done(e);
        }
        if (cntr === urlsToBeStore.length) {
          // console.log('urlsToBeStore store done! check urlsToBeStore', currDepth,urlsToBeStore)
          levelCrawl(urlKeys);
          // doMapReduce();
        }
      });
    });

  }
  crawl();


}, 300000);

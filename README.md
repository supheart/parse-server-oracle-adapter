# parse-server-oracle-adapter
This is database adapter to add support of Oracle to Parse Server

INSTALL
npm install parse-server-oracle-adapter

USE
```javascript
const http = require('http');
const express = require('express');
const ParseServer = require('parse-server').ParseServer;
const Oracle = require('parse-server-oracle-adapter');
const FSFilesAdapter = require('@parse/fs-files-adapter');

const oracleUri = 'oracle://user:password@host:port/database';
const oracle = new Oracle(oracleUri);
// upload file adapter
const fsAdapter = new FSFilesAdapter({
  filesSubDirectory: '../folder' // optional
});
const adapter = oracle.getAdapter();

const configs = {
  databaseAdapter: adapter,
  filesAdapter: fsAdapter,
  appId: 'appid',
  masterKey: 'master_key'
};

const api = new ParseServer(configs);

const app = express();

app.use('/parse', api);

const port = 1337;
const httpServer = http.createServer(app);
httpServer.listen(port, () => {
  console.log(`parse server start, running on port: ${port}`);
});
```

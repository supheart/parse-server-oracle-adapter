const Adapter = require('./Adapter');

class Oracle {
  constructor(uri, options) {
    this._uri = uri;
    this._options = options || {};
  }

  getAdapter() {
    if (this._adapter) {
      return this._adapter;
    }

    this._adapter = new Adapter({
      uri: this._uri,
      collectionPrefix: '',
      databaseOptionis: this._options
    });

    return this._adapter;
  }
}

module.exports = Oracle;

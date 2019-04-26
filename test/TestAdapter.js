const Oracle = require('../src/index');

class TestAdapter {
  constructor() {
    const url = '';
    this._adapter = new Oracle(url);
  }
  getAdapter() {
    return this._adapter.getAdapter();
  }
}
module.exports = TestAdapter;

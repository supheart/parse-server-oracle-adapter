'use strict';

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var Adapter = require('./Adapter');

var Oracle = function () {
  function Oracle(uri, options) {
    _classCallCheck(this, Oracle);

    this._uri = uri;
    this._options = options || {};
  }

  _createClass(Oracle, [{
    key: 'getAdapter',
    value: function getAdapter() {
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
  }]);

  return Oracle;
}();

module.exports = Oracle;
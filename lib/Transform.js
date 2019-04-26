'use strict';

var _extends2 = require('babel-runtime/helpers/extends');

var _extends3 = _interopRequireDefault(_extends2);

var _freeze = require('babel-runtime/core-js/object/freeze');

var _freeze2 = _interopRequireDefault(_freeze);

var _stringify = require('babel-runtime/core-js/json/stringify');

var _stringify2 = _interopRequireDefault(_stringify);

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

// Duplicate from then mongo adapter...
var parseTypeToMySQLType = function parseTypeToMySQLType(type) {
  switch (type.type) {
    case 'String':
      return 'text';
    case 'Date':
      return 'timestamp(6)';
    case 'Object':
      return 'json';
    case 'File':
      return 'text';
    case 'Boolean':
      return 'boolean';
    case 'Pointer':
      return 'char(10)';
    case 'Number':
      return 'double precision';
    case 'GeoPoint':
      return 'point';
    case 'Bytes':
      return 'json';
    case 'Array':
      return 'json';
    default:
      throw 'no type for ' + (0, _stringify2.default)(type) + ' yet';
  }
};

var emptyCLPS = (0, _freeze2.default)({
  find: {},
  get: {},
  create: {},
  update: {},
  delete: {},
  addField: {}
});

var defaultCLPS = (0, _freeze2.default)({
  find: { '*': true },
  get: { '*': true },
  create: { '*': true },
  update: { '*': true },
  delete: { '*': true },
  addField: { '*': true }
});

var toParseSchema = function toParseSchema(schema) {
  if (schema.className === '_User') {
    delete schema.fields._hashed_password;
  }
  if (schema.fields) {
    delete schema.fields._wperm;
    delete schema.fields._rperm;
  }
  var clps = defaultCLPS;
  if (schema.classLevelPermissions) {
    clps = (0, _extends3.default)({}, emptyCLPS, schema.classLevelPermissions);
  }
  return {
    className: schema.className,
    fields: schema.fields,
    classLevelPermissions: clps
  };
};

module.exports = {
  toParseSchema: toParseSchema,
  parseTypeToMySQLType: parseTypeToMySQLType
};
'use strict';

var url = require('url');
var log = require('npmlog');
var Parse = require('parse/node');

var LOG_PREFIX = 'oracle-adapter: ';

var debug = function debug(name, args) {
  if (global.isInitialized) {
    return;
  }
  // console.log(LOG_PREFIX, name);
  log.info(LOG_PREFIX, name);
  if (args) {
    log.info(LOG_PREFIX, ' args:', args);
  }
};

var replaceVarn = function replaceVarn(sql, params) {
  var result = sql;
  for (var i in params) {
    var reg = new RegExp(':' + i + ':', 'g');
    result = result.replace(reg, params[i]);
  }
  return result;
};

var turnSqlParams = function turnSqlParams(values, key) {
  var index = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 1;

  var valuesObj = {};
  values.forEach(function (e, i) {
    valuesObj['' + key + (i + index)] = e;
  });
  return valuesObj;
};

var getSqlTextByArray = function getSqlTextByArray(sql, values) {
  var key = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : 'name';
  var index = arguments.length > 3 && arguments[3] !== undefined ? arguments[3] : 1;
  return replaceVarn(sql, turnSqlParams(values, key, index));
};

var queryFormat = function queryFormat(query, values) {
  if (!values) return query;
  var maxVariable = 100000;
  var multipleValues = /\$([1-9][0-9]{0,16}(?![0-9])(\^|~|#|:raw|:alias|:name|:json|:csv|:value)?)/g;
  var validModifiers = /\^|~|#|:raw|:alias|:name|:json|:csv|:value/;
  var sql = query.replace(multipleValues, function (name) {
    var mod = name.substring(1).match(validModifiers);
    var idx = 100000;
    if (!mod) {
      idx = name.substring(1) - 1;
    } else {
      idx = name.substring(1).substring(0, mod.index) - 1;
    }
    if (idx >= maxVariable) {
      throw new Parse.Error('Variable $' + name.substring(1) + ' exceeds supported maximum of $' + maxVariable);
    }
    if (idx < values.length) {
      return values[idx];
    }
    throw new Parse.Error('Index $' + idx + ' exceeds values length: $' + values.length);
  });
  return sql;
};

var parseQueryParams = function parseQueryParams(queryString) {
  queryString = queryString || '';

  return queryString.split('&').reduce(function (p, c) {
    var parts = c.split('=');
    p[decodeURIComponent(parts[0])] = parts.length > 1 ? decodeURIComponent(parts.slice(1).join('=')) : '';
    return p;
  }, {});
};

var getDatabaseOptionsFromURI = function getDatabaseOptionsFromURI(uri) {
  var databaseOptions = {};

  var parsedURI = url.parse(uri);
  var queryParams = parseQueryParams(parsedURI.query);
  var authParts = parsedURI.auth ? parsedURI.auth.split(':') : [];

  databaseOptions.host = parsedURI.hostname || 'localhost';
  databaseOptions.port = parsedURI.port ? parseInt(parsedURI.port, 10) : 5432;
  databaseOptions.database = parsedURI.pathname ? parsedURI.pathname.substr(1) : undefined;

  databaseOptions.user = authParts.length > 0 ? authParts[0] : '';
  databaseOptions.password = authParts.length > 1 ? authParts[1] : '';

  databaseOptions.ssl = queryParams.ssl && queryParams.ssl.toLowerCase() === 'true';
  databaseOptions.binary = queryParams.binary && queryParams.binary.toLowerCase() === 'true';

  databaseOptions.client_encoding = queryParams.client_encoding;
  databaseOptions.application_name = queryParams.application_name;
  databaseOptions.fallback_application_name = queryParams.fallback_application_name;

  if (queryParams.poolSize) {
    databaseOptions.poolSize = parseInt(queryParams.poolSize, 10) || 10;
  }

  return databaseOptions;
};

module.exports = {
  debug: debug,
  replaceVarn: replaceVarn,
  turnSqlParams: turnSqlParams,
  getSqlTextByArray: getSqlTextByArray,
  queryFormat: queryFormat,
  parseQueryParams: parseQueryParams,
  getDatabaseOptionsFromURI: getDatabaseOptionsFromURI
};
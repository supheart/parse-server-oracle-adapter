const url = require('url');
const log = require('npmlog');
const Parse = require('parse/node');

const LOG_PREFIX = 'oracle-adapter: ';

const debug = (name, args) => {
  if (global.isInitialized) {
    return;
  }
  // console.log(LOG_PREFIX, name);
  log.info(LOG_PREFIX, name);
  if (args) {
    log.info(LOG_PREFIX, ' args:', args);
  }
};

const replaceVarn = (sql, params) => {
  let result = sql;
  for (const i in params) {
    const reg = new RegExp(`:${i}:`, 'g');
    result = result.replace(reg, params[i]);
  }
  return result;
};

const turnSqlParams = (values, key, index = 1) => {
  const valuesObj = {};
  values.forEach((e, i) => {
    valuesObj[`${key}${i + index}`] = e;
  });
  return valuesObj;
};

const getSqlTextByArray = (sql, values, key = 'name', index = 1) => replaceVarn(sql, turnSqlParams(values, key, index));

const queryFormat = (query, values) => {
  if (!values) return query;
  const maxVariable = 100000;
  const multipleValues = /\$([1-9][0-9]{0,16}(?![0-9])(\^|~|#|:raw|:alias|:name|:json|:csv|:value)?)/g;
  const validModifiers = /\^|~|#|:raw|:alias|:name|:json|:csv|:value/;
  const sql = query.replace(multipleValues, (name) => {
    const mod = name.substring(1).match(validModifiers);
    let idx = 100000;
    if (!mod) {
      idx = name.substring(1) - 1;
    } else {
      idx = name.substring(1).substring(0, mod.index) - 1;
    }
    if (idx >= maxVariable) {
      throw new Parse.Error(`Variable $${name.substring(1)} exceeds supported maximum of $${maxVariable}`);
    }
    if (idx < values.length) {
      return values[idx];
    }
    throw new Parse.Error(`Index $${idx} exceeds values length: $${values.length}`);
  });
  return sql;
};

const parseQueryParams = (queryString) => {
  queryString = queryString || '';

  return queryString
    .split('&')
    .reduce((p, c) => {
      const parts = c.split('=');
      p[decodeURIComponent(parts[0])] =
        parts.length > 1
          ? decodeURIComponent(parts.slice(1).join('='))
          : '';
      return p;
    }, {});
};

const getDatabaseOptionsFromURI = (uri) => {
  const databaseOptions = {};

  const parsedURI = url.parse(uri);
  const queryParams = parseQueryParams(parsedURI.query);
  const authParts = parsedURI.auth ? parsedURI.auth.split(':') : [];

  databaseOptions.host = parsedURI.hostname || 'localhost';
  databaseOptions.port = parsedURI.port ? parseInt(parsedURI.port, 10) : 5432;
  databaseOptions.database = parsedURI.pathname
    ? parsedURI.pathname.substr(1)
    : undefined;

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
  debug,
  replaceVarn,
  turnSqlParams,
  getSqlTextByArray,
  queryFormat,
  parseQueryParams,
  getDatabaseOptionsFromURI
};

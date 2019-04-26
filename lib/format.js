'use strict';

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; };

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

var Parse = require('parse/node');
// const _ = require('lodash');

// Duplicate from then mongo adapter...
var parseTypeToOracleType = function parseTypeToOracleType(type) {
  switch (type.type) {
    case 'String':
      return 'varchar2(2000)';
    case 'Date':
      return 'timestamp(6)';
    case 'Object':
      return 'varchar2(4000)';
    case 'File':
      return 'varchar2(2000)';
    case 'Boolean':
      return 'number(1)';
    case 'Pointer':
      return 'varchar(16)';
    case 'Number':
      return 'double precision';
    case 'GeoPoint':
      return 'varchar(32)';
    case 'Bytes':
      return 'blob';
    case 'Array':
      return 'varchar2(4000)';
    default:
      throw 'no type for ' + JSON.stringify(type) + ' yet';
  }
};

var emptyCLPS = Object.freeze({
  find: {},
  get: {},
  create: {},
  update: {},
  delete: {},
  addField: {}
});

var defaultCLPS = Object.freeze({
  find: { '*': true },
  get: { '*': true },
  create: { '*': true },
  update: { '*': true },
  delete: { '*': true },
  addField: { '*': true }
});

var ParseToOracleComparator = {
  $gt: '>',
  $lt: '<',
  $gte: '>=',
  $lte: '<='
};

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
    clps = _extends({}, emptyCLPS, schema.classLevelPermissions);
  }
  return {
    className: schema.className,
    fields: schema.fields,
    classLevelPermissions: clps
  };
};

var toOracleSchema = function toOracleSchema(schema) {
  if (!schema) {
    return schema;
  }
  schema.fields = schema.fields || {};
  schema.fields._wperm = { type: 'Array', contents: { type: 'String' } };
  schema.fields._rperm = { type: 'Array', contents: { type: 'String' } };
  if (schema.className === '_User') {
    schema.fields._hashed_password = { type: 'String' };
    schema.fields._password_history = { type: 'Array' };
  }
  return schema;
};

var formatDateToOracle = function formatDateToOracle(value) {
  var encoded = Parse._encode(new Date(value));
  encoded.iso = encoded.iso.replace('T', ' ').replace('Z', '');
  if (!value.iso) {
    return 'TO_TIMESTAMP(\'' + encoded.iso + '\', \'SYYYY-MM-DD HH24:MI:SS.ff\')';
  }
  return encoded;
};

var toOracleValue = function toOracleValue(value) {
  if ((typeof value === 'undefined' ? 'undefined' : _typeof(value)) === 'object') {
    if (value.__type === 'Date') {
      if (!value.iso) {
        return null;
      }
      return formatDateToOracle(value.iso);
    }
    if (value.__type === 'File') {
      return value.name;
    }
  }
  return value;
};

var transformValue = function transformValue(value) {
  if ((typeof value === 'undefined' ? 'undefined' : _typeof(value)) === 'object' && value.__type === 'Pointer') {
    return value.objectId;
  }
  return value;
};

var removeWhiteSpace = function removeWhiteSpace(regex) {
  if (!regex.endsWith('\n')) {
    regex += '\n';
  }

  // remove non escaped comments
  return regex.replace(/([^\\])#.*\n/gmi, '$1')
  // remove lines starting with a comment
  .replace(/^#.*\n/gmi, '')
  // remove non escaped whitespace
  .replace(/([^\\])\s+/gmi, '$1')
  // remove whitespace at the beginning of a line
  .replace(/^\s+/, '').trim();
};

function createLiteralRegex(remaining) {
  return remaining.split('').map(function (c) {
    if (c.match(/[0-9a-zA-Z]/) !== null) {
      // don't escape alphanumeric characters
      return c;
    }
    // escape everything else (single quotes with single quotes, everything else with a backslash)
    return c === '\'' ? '\'\'' : '\\' + c;
  }).join('');
}

function literalizeRegexPart(s) {
  var matcher1 = /\\Q((?!\\E).*)\\E$/;
  var result1 = s.match(matcher1);
  if (result1 && result1.length > 1 && result1.index > -1) {
    // process regex that has a beginning and an end specified for the literal text
    var prefix = s.substr(0, result1.index);
    var remaining = result1[1];

    return literalizeRegexPart(prefix) + createLiteralRegex(remaining);
  }

  // process regex that has a beginning specified for the literal text
  var matcher2 = /\\Q((?!\\E).*)$/;
  var result2 = s.match(matcher2);
  if (result2 && result2.length > 1 && result2.index > -1) {
    var _prefix = s.substr(0, result2.index);
    var _remaining = result2[1];

    return literalizeRegexPart(_prefix) + createLiteralRegex(_remaining);
  }

  // remove all instances of \Q and \E from the remaining text & escape single quotes
  return s.replace(/([^\\])(\\E)/, '$1').replace(/([^\\])(\\Q)/, '$1').replace(/^\\E/, '').replace(/^\\Q/, '').replace(/([^'])'/, '$1\'\'').replace(/^'([^'])/, '\'\'$1').replace('\\w', '[0-9a-zA-Z]');
}

function processRegexPattern(s) {
  if (s && s.startsWith('^')) {
    // regex for startsWith
    return '^' + literalizeRegexPart(s.slice(1));
  } else if (s && s.endsWith('$')) {
    // regex for endsWith
    return literalizeRegexPart(s.slice(0, s.length - 1)) + '$';
  }

  // regex for contains
  return literalizeRegexPart(s);
}

var buildWhereClause = function buildWhereClause(_ref) {
  var schema = _ref.schema,
      query = _ref.query,
      index = _ref.index;

  var patterns = [];
  var values = [];
  var sorts = [];

  schema = toOracleSchema(schema);
  Object.keys(query).forEach(function (fieldName) {
    var isArrayField = schema.fields && schema.fields[fieldName] && schema.fields[fieldName].type === 'Array';
    // const initialPatternsLength = patterns.length;
    var fieldValue = query[fieldName];

    // nothingin the schema, it's gonna blow up
    if (!schema.fields[fieldName]) {
      // as it won't exist
      if (fieldValue && fieldValue.$exists === false) {
        return;
      }
    }

    if (fieldName.indexOf('.') >= 0) {
      var components = fieldName.split('.');
      var name = void 0;
      components.forEach(function (cmpt, idx) {
        if (idx === 0) {
          name = '`' + cmpt + '`->>';
        } else if (idx === 1) {
          name += '\'$.' + cmpt;
        } else {
          name += '.' + cmpt;
        }
      });
      name += "'";
      if (fieldValue === null) {
        patterns.push('`' + name + '` IS NULL');
      } else {
        patterns.push(name + ' = \'' + fieldValue + '\'');
      }
    } else if (fieldValue === null) {
      patterns.push('":name' + index + ':" IS NULL');
      values.push(fieldName);
      index += 1;
      return;
    } else if (typeof fieldValue === 'string') {
      patterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
      values.push(fieldName, fieldValue);
      index += 2;
    } else if (typeof fieldValue === 'boolean') {
      patterns.push('":name' + index + ':" = :name' + (index + 1) + ':');
      values.push(fieldName, fieldValue);
      index += 2;
    } else if (typeof fieldValue === 'number') {
      patterns.push('":name' + index + ':" = :name' + (index + 1) + ':');
      values.push(fieldName, fieldValue);
      index += 2;
    } else if (fieldName === '$or' || fieldName === '$and') {
      var _values;

      var clauses = [];
      var clauseValues = [];
      fieldValue.forEach(function (subQuery) {
        var clause = buildWhereClause({ schema: schema, query: subQuery, index: index });
        if (clause.pattern.length > 0) {
          clauses.push(clause.pattern);
          clauseValues.push.apply(clauseValues, _toConsumableArray(clause.values));
          index += clause.values.length;
        }
      });
      var orOrAnd = fieldName === '$or' ? ' OR ' : ' AND ';
      patterns.push('(' + clauses.join(orOrAnd) + ')');
      (_values = values).push.apply(_values, clauseValues);
    }

    if (fieldValue.$ne !== undefined) {
      if (isArrayField) {
        fieldValue.$ne = JSON.stringify([fieldValue.$ne]);
        patterns.push('JSON_CONTAINS(":name' + index + ':", \':name' + (index + 1) + ':\') != 1');
      } else {
        if (fieldValue.$ne === null) {
          patterns.push('":name' + index + ':" IS NOT NULL');
          values.push(fieldName);
          index += 1;
          return;
        }
        // if not null, we need to manually exclude null
        patterns.push('(":name' + index + ':" <> \':name' + (index + 1) + ':\' OR ":name' + index + ':" IS NULL)');
      }
      // TODO: support arrays
      values.push(fieldName, fieldValue.$ne);
      index += 2;
    }

    if (fieldValue.$eq) {
      patterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
      values.push(fieldName, fieldValue.$eq);
      index += 2;
    }
    // TODO，处理读写权限的问题
    // const isInOrNin = Array.isArray(fieldValue.$in) || Array.isArray(fieldValue.$nin);
    // if (Array.isArray(fieldValue.$in) &&
    //   isArrayField &&
    //   schema.fields[fieldName].contents &&
    //   schema.fields[fieldName].contents.type === 'String') {
    //   const inPatterns = [];
    //   let allowNull = false;
    //   values.push(fieldName);
    //   fieldValue.$in.forEach((listElem, listIndex) => {
    //     if (listElem === null) {
    //       allowNull = true;
    //     } else {
    //       values.push(listElem);
    //       inPatterns.push(`JSON_CONTAINS(":name${index}:", JSON_ARRAY(':name${(index + 1 + listIndex) - (allowNull ? 1 : 0)}:')) = 1`);
    //     }
    //   });
    //   const tempInPattern = inPatterns.join(' OR ');
    //   if (allowNull) {
    //     patterns.push(`(":name${index}:" IS NULL OR ${tempInPattern})`);
    //   } else {
    //     patterns.push(`":name${index}:" && ${tempInPattern}`);
    //   }
    //   index = index + 1 + inPatterns.length;
    // } else if (isInOrNin) {
    //   const createConstraint = (baseArray, notIn) => {
    //     if (baseArray.length > 0) {
    //       const not = notIn ? ' NOT ' : '';
    //       if (isArrayField) {
    //         const operator = notIn ? ' != ' : ' = ';
    //         const inPatterns = [];
    //         values.push(fieldName);
    //         baseArray.forEach((listElem, listIndex) => {
    //           values.push(JSON.stringify(listElem));
    //           inPatterns.push(`JSON_CONTAINS(":name${index}:", '$${index + 1 + listIndex}:name') ${operator} 1`);
    //         });
    //         patterns.push(`${inPatterns.join(' || ')}`);
    //         index = index + 1 + inPatterns.length;
    //       } else {
    //         const inPatterns = [];
    //         values.push(fieldName);
    //         baseArray.forEach((listElem, listIndex) => {
    //           values.push(listElem);
    //           inPatterns.push(`'$${index + 1 + listIndex}:name'`);
    //         });
    //         patterns.push(`":name${index}:" ${not} IN (${inPatterns.join(',')})`);
    //         index = index + 1 + inPatterns.length;
    //       }
    //     } else if (!notIn) {
    //       values.push(fieldName);
    //       patterns.push(`":name${index}:" IS NULL`);
    //       index += 1;
    //     }
    //   };
    //   if (fieldValue.$in) {
    //     createConstraint(_.flatMap(fieldValue.$in, elt => elt), false);
    //   }
    //   if (fieldValue.$nin) {
    //     createConstraint(_.flatMap(fieldValue.$nin, elt => elt), true);
    //   }
    // }

    // if (Array.isArray(fieldValue.$all) && isArrayField) {
    //   patterns.push(`JSON_CONTAINS(":name${index}:", ':name${index + 1}:') = 1`);
    //   values.push(fieldName, JSON.stringify(fieldValue.$all));
    //   index += 2;
    // }

    if (typeof fieldValue.$exists !== 'undefined') {
      if (fieldValue.$exists) {
        patterns.push('":name' + index + ':" IS NOT NULL');
      } else {
        patterns.push('":name' + index + ':" IS NULL');
      }
      values.push(fieldName);
      index += 1;
    }

    if (fieldValue.$text) {
      var search = fieldValue.$text.$search;
      if ((typeof search === 'undefined' ? 'undefined' : _typeof(search)) !== 'object') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $text: $search, should be object');
      }
      if (!search.$term || typeof search.$term !== 'string') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $text: $term, should be string');
      }
      if (search.$language && typeof search.$language !== 'string') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $text: $language, should be string');
      }
      if (search.$caseSensitive && typeof search.$caseSensitive !== 'boolean') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $text: $caseSensitive, should be boolean');
      } else if (search.$caseSensitive) {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $text: $caseSensitive not supported, please use $regex or create a separate lower case column.');
      }
      if (search.$diacriticSensitive && typeof search.$diacriticSensitive !== 'boolean') {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $text: $diacriticSensitive, should be boolean');
      } else if (search.$diacriticSensitive === false) {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $text: $diacriticSensitive - false not supported, install Oracle Unaccent Extension');
      }
      patterns.push('MATCH (":name' + index + ':") AGAINST (\':name' + (index + 1) + ':\')');
      values.push(fieldName, search.$term);
      index += 2;
    }

    if (fieldValue.$nearSphere) {
      var point = fieldValue.$nearSphere;
      sorts.push('ST_Distance_Sphere(":name' + index + ':", ST_GeomFromText(\'POINT(:name' + (index + 1) + ': $' + (index + 2) + ':name)\')) ASC');

      if (fieldValue.$maxDistance) {
        var distance = fieldValue.$maxDistance;
        var distanceInKM = distance * 6371 * 1000;
        patterns.push('ST_Distance_Sphere(":name' + index + ':", ST_GeomFromText(\'POINT(:name' + (index + 1) + ': $' + (index + 2) + ':name)\')) <= $' + (index + 3) + ':name');
        values.push(fieldName, point.longitude, point.latitude, distanceInKM);
        index += 4;
      } else {
        patterns.push('ST_Distance_Sphere(":name' + index + ':", ST_GeomFromText(\'POINT(:name' + (index + 1) + ': $' + (index + 2) + ':name)\'))');
        values.push(fieldName, point.longitude, point.latitude);
        index += 3;
      }
    }

    if (fieldValue.$within && fieldValue.$within.$box) {
      var box = fieldValue.$within.$box;
      var left = box[0].longitude;
      var bottom = box[0].latitude;
      var right = box[1].longitude;
      var top = box[1].latitude;

      patterns.push('MBRCovers(ST_GeomFromText(\'Polygon(:name' + index + ':)\'), `:name' + (index + 1) + ':`)');
      values.push('(' + left + ' ' + bottom + ', ' + left + ' ' + top + ', ' + top + ' ' + right + ', ' + right + ' ' + bottom + ', ' + left + ' ' + bottom + ')', fieldName);
      index += 2;
    }

    if (fieldValue.$geoWithin && fieldValue.$geoWithin.$polygon) {
      var polygon = fieldValue.$geoWithin.$polygon;
      if (!(polygon instanceof Array)) {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $geoWithin value; $polygon should contain at least 3 GeoPoints');
      }
      if (polygon.length < 3) {
        throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $geoWithin value; $polygon should contain at least 3 GeoPoints');
      }
      if (polygon[0].latitude !== polygon[polygon.length - 1].latitude || polygon[0].longitude !== polygon[polygon.length - 1].longitude) {
        polygon.push(polygon[0]);
      }
      var points = polygon.map(function (point) {
        if ((typeof point === 'undefined' ? 'undefined' : _typeof(point)) !== 'object' || point.__type !== 'GeoPoint') {
          throw new Parse.Error(Parse.Error.INVALID_JSON, 'bad $geoWithin value');
        } else {
          Parse.GeoPoint._validate(point.latitude, point.longitude);
        }
        return point.longitude + ' ' + point.latitude;
      }).join(', ');

      patterns.push('MBRCovers(ST_GeomFromText(\'Polygon(:name' + index + ':)\'), `:name' + (index + 1) + ':`)');
      values.push('(' + points + ')', fieldName);
      index += 2;
    }

    if (fieldValue.$regex) {
      var regex = fieldValue.$regex;
      var operator = 'REGEXP';
      var opts = fieldValue.$options;
      if (opts) {
        // if (opts.indexOf('i') >= 0) {
        //   operator = '~*';
        // }
        if (opts.indexOf('x') >= 0) {
          regex = removeWhiteSpace(regex);
        }
      }

      regex = processRegexPattern(regex);

      patterns.push('":name' + index + ':" ' + operator + ' \':name' + (index + 1) + ':\'');
      values.push(fieldName, regex);
      index += 2;
    }

    if (fieldValue.__type === 'Pointer') {
      if (isArrayField) {
        patterns.push('JSON_CONTAINS(":name' + index + ':", \':name' + (index + 1) + ':\') = 1');
        values.push(fieldName, JSON.stringify([fieldValue]));
        index += 2;
      } else {
        patterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
        values.push(fieldName, fieldValue.objectId);
        index += 2;
      }
    }

    if (fieldValue.__type === 'Date') {
      patterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
      values.push(fieldName, toOracleValue(fieldValue));
      index += 2;
    }

    if (fieldValue.__type === 'GeoPoint') {
      patterns.push('":name' + index + ':" = ST_GeomFromText(\'POINT(:name' + (index + 1) + ': $' + (index + 2) + ':name)\')');
      values.push(fieldName, fieldValue.longitude, fieldValue.latitude);
      index += 3;
    }

    Object.keys(ParseToOracleComparator).forEach(function (cmp) {
      if (fieldValue[cmp]) {
        var OracleComparator = ParseToOracleComparator[cmp];
        patterns.push('":name' + index + ':" ' + OracleComparator + ' \':name' + (index + 1) + ':\'');
        values.push(fieldName, toOracleValue(fieldValue[cmp]));
        index += 2;
      }
    });

    // if (initialPatternsLength === patterns.length) {
    //   throw new Parse.Error(Parse.Error.OPERATION_FORBIDDEN, `Oracle does not support this query type yet ${fieldValue}`);
    // }
  });
  values = values.map(transformValue);
  return { pattern: patterns.join(' AND '), values: values, sorts: sorts };
};

var handleDotFields = function handleDotFields(object) {
  Object.keys(object).forEach(function (fieldName) {
    if (fieldName.indexOf('.') > -1) {
      var components = fieldName.split('.');
      var first = components.shift();
      object[first] = object[first] || {};
      var currentObj = object[first];
      var next = void 0;
      var value = object[fieldName];
      if (value && value.__op === 'Delete') {
        value = undefined;
      }
      /* eslint-disable no-cond-assign */
      while (next = components.shift()) {
        /* eslint-enable no-cond-assign */
        currentObj[next] = currentObj[next] || {};
        if (components.length === 0) {
          currentObj[next] = value;
        }
        currentObj = currentObj[next];
      }
      delete object[fieldName];
    }
  });
  return object;
};

var validateKeys = function validateKeys(object) {
  if ((typeof object === 'undefined' ? 'undefined' : _typeof(object)) !== 'object') {
    return;
  }
  Object.keys(object).forEach(function (key) {
    if (_typeof(object[key]) === 'object') {
      validateKeys(object[key]);
    }

    if (key.includes('$') || key.includes('.')) {
      throw new Parse.Error(Parse.Error.INVALID_NESTED_KEY, "Nested keys should not contain the '$' or '.' characters");
    }
  });
};

// Returns the list of join tables on a schema
var joinTablesForSchema = function joinTablesForSchema(schema) {
  var list = [];
  if (schema) {
    Object.keys(schema.fields).forEach(function (field) {
      if (schema.fields[field].type === 'Relation') {
        list.push('_Join:' + field + ':' + schema.className);
      }
    });
  }
  return list;
};

module.exports = {
  toParseSchema: toParseSchema,
  toOracleSchema: toOracleSchema,
  handleDotFields: handleDotFields,
  validateKeys: validateKeys,
  formatDateToOracle: formatDateToOracle,
  toOracleValue: toOracleValue,
  parseTypeToOracleType: parseTypeToOracleType,
  buildWhereClause: buildWhereClause,
  joinTablesForSchema: joinTablesForSchema
};
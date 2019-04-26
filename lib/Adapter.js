'use strict';

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj; };

var _extends = Object.assign || function (target) { for (var i = 1; i < arguments.length; i++) { var source = arguments[i]; for (var key in source) { if (Object.prototype.hasOwnProperty.call(source, key)) { target[key] = source[key]; } } } return target; };

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var oracledb = require('oracledb');

var _require = require('./config'),
    defaultUniqueKeyLength = _require.defaultUniqueKeyLength;

var _require2 = require('./util'),
    debug = _require2.debug,
    getSqlTextByArray = _require2.getSqlTextByArray,
    getDatabaseOptionsFromURI = _require2.getDatabaseOptionsFromURI;

var _require3 = require('./format'),
    toParseSchema = _require3.toParseSchema,
    toOracleSchema = _require3.toOracleSchema,
    handleDotFields = _require3.handleDotFields,
    validateKeys = _require3.validateKeys,
    formatDateToOracle = _require3.formatDateToOracle,
    toOracleValue = _require3.toOracleValue,
    parseTypeToOracleType = _require3.parseTypeToOracleType,
    buildWhereClause = _require3.buildWhereClause,
    joinTablesForSchema = _require3.joinTablesForSchema;

var TABLE_OWNER = 'MORIA';
oracledb.autoCommit = true;

var Adapter = function () {
  function Adapter(_ref) {
    var uri = _ref.uri,
        _ref$collectionPrefix = _ref.collectionPrefix,
        collectionPrefix = _ref$collectionPrefix === undefined ? '' : _ref$collectionPrefix,
        _ref$databaseOptions = _ref.databaseOptions,
        databaseOptions = _ref$databaseOptions === undefined ? {} : _ref$databaseOptions;

    _classCallCheck(this, Adapter);

    this._uri = uri;
    this._collectionPrefix = collectionPrefix;

    var dbOptions = {};
    var options = databaseOptions || {};
    if (uri) {
      dbOptions = getDatabaseOptionsFromURI(uri);
      dbOptions.connectionString = dbOptions.host + ':' + dbOptions.port + '/' + dbOptions.database;
    }
    Object.keys(options).forEach(function (key) {
      dbOptions[key] = options[key];
    });

    dbOptions.multipleStatements = true;
    this._databaseOptions = dbOptions;
    this.canSortOnJoinTables = false;
  }

  // 连接数据库


  _createClass(Adapter, [{
    key: 'connect',
    value: function connect() {
      var _this = this;

      if (this.connectionPromise) {
        return this.connectionPromise;
      }
      this.connectionPromise = oracledb.getConnection(this._databaseOptions).then(function (conn) {
        _this.conn = conn;
      }).catch(function (error) {
        delete _this.connectionPromise;
        return Promise.reject(error);
      });
      return this.connectionPromise;
    }

    // 结束关闭连接

  }, {
    key: 'handleShutdown',
    value: function handleShutdown() {
      if (!this.conn) {
        return;
      }
      this.conn.close();
    }

    // 确认schema表是否存在

  }, {
    key: '_ensureSchemaCollectionExists',
    value: function _ensureSchemaCollectionExists() {
      var _this2 = this;

      debug('_ensureSchemaCollectionExists');

      var sqlText = 'CREATE TABLE "_SCHEMA" ("className" VARCHAR2(120 BYTE) NOT NULL, PRIMARY KEY("className"), "schema" VARCHAR2(4000), "isParseClass" NUMBER(1))';
      return this.classExists('_SCHEMA').then(function (es) {
        return !es ? _this2.conn.execute(sqlText) : Promise.resolve();
      });
    }

    // 创建表

  }, {
    key: 'createTable',
    value: function createTable(className, schema) {
      var _this3 = this;

      debug('createTable', className, schema);

      // 自动更新时间字段的触发器
      // const triggerSql = `CREATE OR REPLACE trigger "${className}_TR"
      //                         BEFORE INSERT OR UPDATE ON "${className}" FOR EACH ROW
      //                         BEGIN
      //                           IF UPDATING THEN
      //                             :NEW.updatedAt := CURRENT_TIMESTAMP(6);
      //                           END IF;
      //                         END;`;

      var valuesArray = [];
      var patternsArray = [];
      var fields = Object.assign({}, schema.fields);
      if (className === '_User') {
        fields._email_verify_token_expires_at = { type: 'Date' };
        fields._email_verify_token = { type: 'String' };
        fields._account_lockout_expires_at = { type: 'Date' };
        fields._failed_login_count = { type: 'Number' };
        fields._perishable_token = { type: 'String' };
        fields._perishable_token_expires_at = { type: 'Date' };
        fields._password_changed_at = { type: 'Date' };
        fields._password_history = { type: 'Array' };
      }
      var index = 2;
      var relations = [];
      Object.keys(fields).forEach(function (fieldName) {
        var parseType = fields[fieldName];
        // Skip when it's a relation
        // We'll create the tables later
        if (parseType.type === 'Relation') {
          relations.push(fieldName);
          return;
        }
        if (['_rperm', '_wperm'].indexOf(fieldName) >= 0) {
          parseType.contents = { type: 'String' };
        }
        valuesArray.push(fieldName);
        if (parseType.type === 'Date') {
          valuesArray.push(parseTypeToOracleType(parseType));
          if (fieldName === 'createdAt') {
            patternsArray.push('":varn' + index + ':" :varn' + (index + 1) + ': DEFAULT CURRENT_TIMESTAMP(6)');
          } else if (fieldName === 'updatedAt') {
            patternsArray.push('":varn' + index + ':" :varn' + (index + 1) + ': DEFAULT CURRENT_TIMESTAMP(6)');
          } else {
            patternsArray.push('":varn' + index + ':" :varn' + (index + 1) + ': NULL');
          }
          index += 2;
          return;
        }
        patternsArray.push('":varn' + index + ':" :varn' + (index + 1) + ':');
        if (fieldName === 'objectId') {
          valuesArray.push(defaultUniqueKeyLength);
          patternsArray.push('PRIMARY KEY (":varn' + index + ':")');
        } else if (fieldName === 'email' || fieldName === 'username' || fieldName === 'name') {
          valuesArray.push(defaultUniqueKeyLength);
        } else {
          valuesArray.push(parseTypeToOracleType(parseType));
        }
        index += 2;
      });

      var qs = 'CREATE TABLE ":varn1:" (' + patternsArray.join(',') + ')';
      var sqlText = getSqlTextByArray(qs, [className].concat(valuesArray), 'varn');

      return this.connect().then(function () {
        return _this3.classExists(className);
      }).then(function (existsTable) {
        if (!existsTable) {
          // TODO 自动更新时间字段
          // if (Object.keys(fields).indexOf('updatedAt') > -1) {
          //   await this.conn.execute(triggerSql);
          // }
          return _this3.conn.execute(sqlText);
        }
      }).then(function () {
        var promises = relations.map(function (filedName) {
          var joinTableName = '_Join:' + filedName + ':' + className;
          var joinTableSql = 'CREATE TABLE "' + joinTableName + '" ("relatedId" varChar(120), "owningId" varChar(120), PRIMARY KEY("relatedId", "owningId"))';
          return _this3.classExists(joinTableName).then(function (etj) {
            return !etj ? _this3.conn.execute(joinTableSql) : Promise.resolve();
          });
        });
        return Promise.all(promises);
      }).catch(function (err) {
        return console.log(err);
      });
    }

    // 获取JSON类型的字段内容

  }, {
    key: '_getJSONSelectQuery',
    value: function _getJSONSelectQuery(className, fieldName, query) {
      var wherePattern = '';
      Object.keys(query).forEach(function (field, index) {
        if (index === 0) {
          wherePattern = 'WHERE';
        } else {
          wherePattern += ' AND';
        }
        wherePattern += ' "' + field + '" = \'' + query[field] + '\'';
      });
      return ('SELECT "' + fieldName + '" FROM "' + className + '" ' + wherePattern).trim();
    }
  }, {
    key: 'getJSONValue',
    value: function getJSONValue(className, fieldName, query) {
      var _this4 = this;

      debug('getJSONValue', className, fieldName, query);

      var sqlText = this._getJSONSelectQuery(className, fieldName, query);
      return this.connect().then(function () {
        return _this4.conn.execute(sqlText);
      }).then(function (res) {
        return res.rows[0] ? JSON.parse(res.rows[0]) : {};
      });
    }
  }, {
    key: 'getJSONValues',
    value: function getJSONValues(className, fieldName, query) {
      var _this5 = this;

      debug('getJSONValues', className, fieldName, query);

      var sqlText = this._getJSONSelectQuery(className, fieldName, query);
      return this.connect().then(function () {
        return _this5.conn.execute(sqlText);
      }).then(function (res) {
        var result = [];
        res.rows.forEach(function (row) {
          result.push(row ? JSON.parse(row[fieldName]) : '');
        });
        return result;
      });
    }

    // 判断表是否存在

  }, {
    key: 'classExists',
    value: function classExists(className) {
      var _this6 = this;

      var owner = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : TABLE_OWNER;

      debug('classExists', className);
      var sql = 'select count(1) from all_tables where TABLE_NAME = \'' + className + '\' and OWNER=\'' + owner + '\'';
      return this.connect().then(function () {
        return _this6.conn.execute(sql);
      }).then(function (result) {
        return !!result.rows[0][0];
      }).catch(function () {
        return Promise.resolve(false);
      });
    }
  }, {
    key: 'setClassLevelPermissions',
    value: function setClassLevelPermissions(className, clps) {
      var _this7 = this;

      debug('setClassLevelPermissions', className);

      return this._ensureSchemaCollectionExists().then(function () {
        return _this7.getJSONValue(className, 'schema', { className: className });
      }).then(function (res) {
        res.classLevelPermissions = clps;
        var sqlText = 'UPDATE "_SCHEMA" SET "schema" = \'' + JSON.stringify(res) + '\' WHERE "className" = \'' + className + '\'';
        return _this7.conn.execute(sqlText);
      });
    }
  }, {
    key: 'createClass',
    value: function createClass(className, schema) {
      var _this8 = this;

      debug('createClass', { className: className, schema: schema });

      var qs = 'INSERT INTO "_SCHEMA" ("className", "schema", "isParseClass") VALUES (\'' + schema.className + '\', \'' + JSON.stringify(schema) + '\', 1)';
      return this.createTable(className, schema).then(function () {
        return _this8.conn.execute(qs);
      }).then(function () {
        return toParseSchema(schema);
      }).catch(function (err) {
        throw err;
      });
    }
  }, {
    key: 'addFieldIfNotExists',
    value: function addFieldIfNotExists(className, fieldName, type) {
      var _this9 = this;

      debug('addFieldIfNotExists', { className: className, fieldName: fieldName, type: type });

      var promise = Promise.resolve();
      if (type.type !== 'Relation') {
        var oracleType = parseTypeToOracleType(type);
        if (type.type === 'Date') {
          oracleType = 'timestamp(6) null default null';
        }
        promise = this.conn.execute('ALTER TABLE "' + className + '" ADD ("' + fieldName + '" ' + oracleType + ')');
      } else {
        var relationTable = '_Join:' + fieldName + ':' + className;
        var sq = 'CREATE TABLE "' + relationTable + '" ("relatedId" varChar2(120), "owningId" varChar2(120), PRIMARY KEY("relatedId", "owningId"))';
        promise = this.classExists(relationTable).then(function (et) {
          return !et ? _this9.conn.execute(sq) : Promise.resolve();
        });
      }

      return promise.then(function () {
        // 先查询出当前类的内容
        var schemaQuerySql = 'SELECT "schema" FROM "_SCHEMA" WHERE "className" = \'' + className + '\'';
        return _this9.conn.execute(schemaQuerySql, {}, { outFormat: oracledb.OBJECT });
      }).then(function (schemaResult) {
        var schemaObject = schemaResult.rows[0].schema ? JSON.parse(schemaResult.rows[0].schema) : { fields: {} };
        if (!schemaObject.fields[fieldName]) {
          schemaObject.fields[fieldName] = type;
          var updateSchemaSql = 'UPDATE "_SCHEMA" SET "schema"= \'' + JSON.stringify(schemaObject) + '\' WHERE "className"=\'' + className + '\'';
          return _this9.conn.execute(updateSchemaSql);
        }
        throw 'Attempted to add a field that already exists';
      });
    }
  }, {
    key: 'deleteClass',
    value: function deleteClass(className) {
      var _this10 = this;

      debug('deleteClass', className);

      var dropText = 'DROP TABLE "' + className + '"';
      var deleteText = 'DELETE FROM "_SCHEMA" WHERE "className" = \'' + className + '\';';
      return this.connect().then(function () {
        return _this10.classExists(className);
      }).then(function (et) {
        return !et ? _this10.conn.execute(dropText) : Promise.resolve();
      }).then(function () {
        return _this10.conn.execute(deleteText);
      }).catch(function (error) {
        throw error;
      });
    }
  }, {
    key: 'deleteAllClasses',
    value: function deleteAllClasses(fast) {
      var _this11 = this;

      debug('deleteAllClasses', fast);

      var now = new Date().getTime();
      var selectText = 'SELECT * FROM "_SCHEMA"';
      var originClass = ['_SCHEMA', '_PushStatus', '_JobStatus', '_JobSchedule', '_Hooks', '_GlobalConfig', '_Audience'];
      return this.connect().then(function () {
        return _this11.conn.execute(selectText, {}, { outFormat: oracledb.OBJECT });
      }).then(function (res) {
        var resClass = res.rows.map(function (r) {
          return r.className;
        });
        var joins = res.rows.reduce(function (list, schema) {
          return list.concat(joinTablesForSchema(JSON.parse(schema.schema)));
        }, []);
        var classes = [].concat(originClass, _toConsumableArray(resClass), _toConsumableArray(joins));
        var promises = classes.map(function (c) {
          var sqlText = 'DROP TABLE "' + c + '"';
          return _this11.classExists(c).then(function (et) {
            return et ? _this11.conn.execute(sqlText) : Promise.resolve();
          });
        });
        return Promise.all(promises);
      }).then(function () {
        debug('deleteAllClasses done in ' + (new Date().getTime() - now));
      }).catch(function (error) {
        throw error;
      });
    }
  }, {
    key: 'deleteFields',
    value: function deleteFields(className, schema, fieldNames) {
      var _this12 = this;

      debug('deleteFields', className, schema, fieldNames);

      var fields = fieldNames.reduce(function (list, fieldName) {
        var field = schema.fields[fieldName];
        if (field.type !== 'Relation') {
          list.push(fieldName);
        }
        delete schema.fields[fieldName];
        return list;
      }, []);

      var values = [className].concat(_toConsumableArray(fields));
      var columns = fields.map(function (name, idx) {
        return '":name' + (idx + 2) + ':"';
      }).join(', DROP COLUMN');
      // 这里先取出JSON字段做处理，然后再更改表内容
      return this.getJSONValue('_SCHEMA', 'schema', { className: className }).then(function (res) {
        res.fields = schema.fields;
        var updateSql = 'UPDATE "_SCHEMA" SET "schema" = \'' + JSON.stringify(res) + '\' WHERE "className" = \'' + className + '\'';
        return _this12.conn.execute(updateSql);
      }).then(function () {
        if (values.length > 1) {
          var aqs = 'ALTER TABLE ":name1:" DROP COLUMN ' + columns;
          var alterText = getSqlTextByArray(aqs, values);
          return _this12.conn.execute(alterText);
        }
        return Promise.resolve();
      }).catch(function (error) {
        throw error;
      });
    }
  }, {
    key: 'getAllClasses',
    value: function getAllClasses() {
      var _this13 = this;

      debug('getAllClasses');

      return this._ensureSchemaCollectionExists().then(function () {
        return _this13.conn.execute('SELECT * FROM "_SCHEMA"', {}, { outFormat: oracledb.OBJECT });
      }).then(function (result) {
        return result.rows.map(function (row) {
          return toParseSchema(_extends({ className: row.className }, JSON.parse(row.schema)));
        });
      });
    }
  }, {
    key: 'getClass',
    value: function getClass(className) {
      debug('getClass', className);

      // TODO 原来有按字符排序的sql：SELECT * FROM `_SCHEMA` WHERE `className` COLLATE latin1_general_cs =\'$1:name\
      var classSql = 'SELECT * FROM "_SCHEMA" WHERE "className" = \'' + className + '\'';
      return this.conn.execute(classSql, {}, { outFormat: oracledb.OBJECT }).then(function (result) {
        if (result.rows.length === 1) {
          return JSON.parse(result.rows[0].schema);
        }
        throw undefined;
      }).then(toParseSchema);
    }
  }, {
    key: 'createObject',
    value: function createObject(className, schema, object) {
      debug('createObject', className, object);

      var columnsArray = [];
      var valuesArray = [];
      schema = toOracleSchema(schema);
      var geoPoints = {};
      object = handleDotFields(object);
      validateKeys(object);

      Object.keys(object).forEach(function (fieldName) {
        if (object[fieldName] === null) {
          return;
        }
        var authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
        if (authDataMatch) {
          var provider = authDataMatch[1];
          object.authData = object.authData || {};
          object.authData[provider] = object[fieldName];
          delete object[fieldName];
          fieldName = 'authData';
        }

        columnsArray.push(fieldName);
        if (!schema.fields[fieldName] && className === '_User') {
          if (fieldName === '_email_verify_token' || fieldName === '_failed_login_count' || fieldName === '_perishable_token' || fieldName === '_password_history') {
            valuesArray.push(object[fieldName]);
          }
          if (fieldName === '_account_lockout_expires_at' || fieldName === '_perishable_token_expires_at' || fieldName === '_password_changed_at' || fieldName === '_email_verify_token_expires_at') {
            if (object[fieldName]) {
              valuesArray.push(toOracleValue(object[fieldName]));
            } else {
              valuesArray.push(null);
            }
          }
          return;
        }
        switch (schema.fields[fieldName].type) {
          case 'Date':
            if (object[fieldName]) {
              if (fieldName === 'updatedAt' && !object[fieldName].iso) {
                object[fieldName].iso = new Date();
              }
              valuesArray.push(toOracleValue(object[fieldName]));
            } else {
              valuesArray.push(null);
            }
            break;
          case 'Pointer':
            valuesArray.push(object[fieldName].objectId);
            break;
          case 'Array':
          case 'Object':
          case 'Bytes':
            valuesArray.push(JSON.stringify(object[fieldName]));
            break;
          case 'String':
          case 'Number':
            if (!object[fieldName]) {
              valuesArray.push(0);
              break;
            }
            valuesArray.push(object[fieldName]);
            break;
          case 'Boolean':
            valuesArray.push(object[fieldName] ? 1 : 0);
            break;
          case 'File':
            valuesArray.push(object[fieldName].name);
            break;
          case 'GeoPoint':
            // pop the point and process later
            geoPoints[fieldName] = object[fieldName];
            columnsArray.pop();
            break;
          default:
            throw 'Type ' + schema.fields[fieldName].type + ' not supported yet';
        }
      });

      columnsArray = columnsArray.concat(Object.keys(geoPoints));
      var initialValues = valuesArray.map(function (val, index) {
        var fieldName = columnsArray[index];
        if (schema.fields[fieldName] && (schema.fields[fieldName].type === 'Boolean' || schema.fields[fieldName].type === 'Date') || val === null) {
          return ':name' + (index + 2 + columnsArray.length) + ':';
        }

        return '\':name' + (index + 2 + columnsArray.length) + ':\'';
      });
      var geoPointsInjects = Object.keys(geoPoints).map(function (key) {
        var value = geoPoints[key];
        valuesArray.push(value.longitude, value.latitude);
        var l = valuesArray.length + columnsArray.length;
        return 'POINT(' + l + ', ' + (l + 1) + ')';
      });

      var columnsPattern = columnsArray.map(function (col, index) {
        return '":name' + (index + 2) + ':"';
      }).join(',');
      var valuesPattern = initialValues.concat(geoPointsInjects).join(',');
      var qs = 'INSERT INTO ":name1:" (' + columnsPattern + ') VALUES (' + valuesPattern + ')';
      var values = [className].concat(_toConsumableArray(columnsArray), valuesArray);
      var sqlText = getSqlTextByArray(qs, values);

      return this.conn.execute(sqlText).then(function () {
        return { ops: [object] };
      });
    }
  }, {
    key: 'deleteObjectsByQuery',
    value: function deleteObjectsByQuery(className, schema, query) {
      var _this14 = this;

      debug('deleteObjectsByQuery', className, query);

      var values = [className];
      var index = 2;
      var where = buildWhereClause({ schema: schema, index: index, query: query });
      values.push.apply(values, _toConsumableArray(where.values));
      if (Object.keys(query).length === 0) {
        where.pattern = 'TRUE';
      }
      var qs = 'DELETE FROM ":name1:" WHERE ' + where.pattern;
      var sqlText = getSqlTextByArray(qs, values);

      return this.connect().then(function () {
        return _this14.conn.execute(sqlText);
      }).then(function (res) {
        if (res.rowsAffected === 0) {
          throw 'Object not found.';
        } else {
          return res.rowsAffected;
        }
      });
    }
  }, {
    key: 'updateObjectsByQuery',
    value: function updateObjectsByQuery(className, schema, query, update) {
      var _this15 = this;

      debug('updateObjectsByQuery', className, query, update);

      var updatePatterns = [];
      var values = [className];
      var index = 2;
      schema = toOracleSchema(schema);

      // const originalUpdate = { ...update };
      update = handleDotFields(update);
      // Resolve authData first,
      // So we don't end up with multiple key updates
      Object.keys(update).forEach(function (fieldName) {
        var authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
        if (authDataMatch) {
          var provider = authDataMatch[1];
          var value = update[fieldName];
          delete update[fieldName];
          update.authData = update.authData || {};
          update.authData[provider] = value;
        }
      });

      Object.keys(update).forEach(function (fieldName) {
        var fieldValue = update[fieldName];
        if (fieldValue === null) {
          updatePatterns.push(':name' + index + ': = NULL');
          values.push(fieldName);
          index += 1;
        } else if (fieldName === 'authData') {
          // This recursively sets the json_object
          // Only 1 level deep
          // const getJSONV = this.getJSONValue(className, fieldName, query);
          // TODO 这里对json字段做更新，需要先取值，再修改
          // const generate = (jsonb, key, value) => `JSON_SET(COALESCE(\`${fieldName}\`, '{}'), '$.${key}', CAST('${value}' AS JSON))`;
          // const lastKey = `:name${index}:`;
          // const fieldNameIndex = index;
          // index += 1;
          // values.push(`"${fieldName}"`);
          // const updates = Object.keys(fieldValue).reduce((json, key) => {
          //   const str = generate(json, `:name${index}:`, `:name${index + 1}:`);
          //   index += 2;
          //   let value = fieldValue[key];
          //   if (value) {
          //     if (value.__op === 'Delete') {
          //       value = null;
          //     } else {
          //       value = JSON.stringify(value);
          //     }
          //   }
          //   values.push(key, value);
          //   return str;
          // }, lastKey);
          // updatePatterns.push(`$${fieldNameIndex}:name = ${updates}`);
        } else if (fieldValue.__op === 'Increment') {
          updatePatterns.push('":name' + index + ':" = COALESCE(":name' + index + ':", 0) + :name' + (index + 1) + ':');
          values.push(fieldName, fieldValue.amount);
          index += 2;
        } else if (fieldValue.__op === 'Add') {
          updatePatterns.push('":name' + index + ':"= JSON_ARRAY_INSERT(COALESCE(":name' + index + ':", \'[]\'), CONCAT(\'$[\',JSON_LENGTH(":name' + index + ':"),\']\'), \':name' + (index + 1) + ':\')');
          values.push(fieldName, JSON.stringify(fieldValue.objects));
          index += 2;
        } else if (fieldValue.__op === 'Delete') {
          updatePatterns.push('":name' + index + ':" = $' + (index + 1));
          values.push(fieldName, null);
          index += 2;
        } else if (fieldValue.__op === 'Remove') {
          fieldValue.objects.forEach(function (obj) {
            updatePatterns.push('":name' + index + ':" = JSON_REMOVE(":name' + index + ':", REPLACE(JSON_SEARCH(COALESCE(":name' + index + ':",\'[]\'), \'one\', \':name' + (index + 1) + ':\'),\'"\',\'\'))');
            if ((typeof obj === 'undefined' ? 'undefined' : _typeof(obj)) === 'object') {
              values.push(fieldName, JSON.stringify(obj));
            } else {
              values.push(fieldName, obj);
            }
            index += 2;
          });
        } else if (fieldValue.__op === 'AddUnique') {
          fieldValue.objects.forEach(function (obj) {
            updatePatterns.push('":name' + index + ':" = if (JSON_CONTAINS(":name' + index + ':", \':name' + (index + 1) + ':\') = 0, JSON_MERGE(":name' + index + ':",\':name' + (index + 1) + ':\'),":name' + index + ':")');
            if ((typeof obj === 'undefined' ? 'undefined' : _typeof(obj)) === 'object') {
              values.push(fieldName, JSON.stringify(obj));
            } else {
              values.push(fieldName, obj);
            }
            index += 2;
          });
        } else if (fieldName === 'updatedAt' || fieldName === 'finishedAt') {
          // TODO: stop special casing this. It should check for __type === 'Date' and use .iso
          updatePatterns.push('":name' + index + ':" = :name' + (index + 1) + ':');
          values.push(fieldName, formatDateToOracle(fieldValue));
          index += 2;
        } else if (typeof fieldValue === 'string') {
          updatePatterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
          values.push(fieldName, fieldValue);
          index += 2;
        } else if (typeof fieldValue === 'boolean') {
          updatePatterns.push('":name' + index + ':" = :name' + (index + 1) + ':');
          values.push(fieldName, fieldValue);
          index += 2;
        } else if (fieldValue.__type === 'Pointer') {
          updatePatterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
          values.push(fieldName, fieldValue.objectId);
          index += 2;
        } else if (fieldValue.__type === 'Date') {
          updatePatterns.push('":name' + index + ':" = :name' + (index + 1) + ':');
          values.push(fieldName, toOracleValue(fieldValue));
          index += 2;
        } else if (fieldValue instanceof Date) {
          updatePatterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
          values.push(fieldName, fieldValue);
          index += 2;
        } else if (fieldValue.__type === 'File') {
          updatePatterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
          values.push(fieldName, toOracleValue(fieldValue));
          index += 2;
        } else if (fieldValue.__type === 'GeoPoint') {
          updatePatterns.push('":name' + index + ':" = POINT($' + (index + 1) + ', $' + (index + 2) + ')');
          values.push(fieldName, fieldValue.longitude, fieldValue.latitude);
          index += 3;
        } else if (fieldValue.__type === 'Relation') {
          // noop
        } else if (typeof fieldValue === 'number') {
          updatePatterns.push('":name' + index + ':" = $' + (index + 1));
          values.push(fieldName, fieldValue);
          index += 2;
        } else if ((typeof fieldValue === 'undefined' ? 'undefined' : _typeof(fieldValue)) === 'object' && schema.fields[fieldName] && schema.fields[fieldName].type === 'Object') {
          // const keysToSet = Object.keys(originalUpdate).filter(k =>
          //   // choose top level fields that don't have operation or . (dot) field
          //   !originalUpdate[k].__op && k.indexOf('.') === -1 && k !== 'updatedAt');

          // let setPattern = '';
          // if (keysToSet.length > 0) {
          //   setPattern = keysToSet.map(() => `CAST('${JSON.stringify(fieldValue)}' AS JSON)`);
          // }
          // const keysToReplace = Object.keys(originalUpdate).filter(k =>
          //   // choose top level fields that dont have operation
          //   !originalUpdate[k].__op && k.split('.').length === 2 && k.split('.')[0] === fieldName).map(k => k.split('.')[1]);

          // let replacePattern = '';
          // if (keysToReplace.length > 0) {
          //   replacePattern = keysToReplace.map((c) => {
          //     if (typeof fieldValue[c] === 'object') {
          //       return `'$.${c}', CAST('${JSON.stringify(fieldValue[c])}' AS JSON)`;
          //     }
          //     return `'$.${c}', '${fieldValue[c]}'`;
          //   }).join(' || ');

          //   keysToReplace.forEach((key) => {
          //     delete fieldValue[key];
          //   });
          // }

          // const keysToIncrement = Object.keys(originalUpdate).filter(k =>
          //   // choose top level fields that have a increment operation set
          //   originalUpdate[k].__op === 'Increment' && k.split('.').length === 2 && k.split('.')[0] === fieldName).map(k => k.split('.')[1]);

          // let incrementPatterns = '';
          // if (keysToIncrement.length > 0) {
          //   incrementPatterns = keysToIncrement.map((c) => {
          //     const amount = fieldValue[c].amount;
          //     return `'$.${c}', COALESCE(":name${index}:"->>'$.${c}','0') + ${amount}`;
          //   }).join(' || ');

          //   keysToIncrement.forEach((key) => {
          //     delete fieldValue[key];
          //   });
          // }

          // const keysToDelete = Object.keys(originalUpdate).filter(k =>
          //   // choose top level fields that have a delete operation set
          //   originalUpdate[k].__op === 'Delete' && k.split('.').length === 2 && k.split('.')[0] === fieldName).map(k => k.split('.')[1]);

          // const deletePatterns = keysToDelete.reduce((p, c, i) => `'$.$${index + 1 + i}:name'`, ', ');

          // if (keysToDelete.length > 0) {
          //   updatePatterns.push(`":name${index}:" = JSON_REMOVE(":name${index}:", ${deletePatterns})`);
          // }
          // if (keysToIncrement.length > 0) {
          //   updatePatterns.push(`":name${index}:" = JSON_SET(COALESCE(":name${index}:", '{}'), ${incrementPatterns})`);
          // }
          // if (keysToReplace.length > 0) {
          //   updatePatterns.push(`":name${index}:" = JSON_SET(COALESCE(":name${index}:", '{}'), ${replacePattern})`);
          // }
          // if (keysToSet.length > 0) {
          //   updatePatterns.push(`":name${index}:" = ${setPattern}`);
          // }

          // values.push(fieldName, ...keysToDelete, JSON.stringify(fieldValue));
          // index += 2 + keysToDelete.length;

          updatePatterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
          values.push(fieldName, JSON.stringify(fieldValue));
          index += 2;
        } else if (Array.isArray(fieldValue) && schema.fields[fieldName] && schema.fields[fieldName].type === 'Array') {
          updatePatterns.push('":name' + index + ':" = \':name' + (index + 1) + ':\'');
          values.push(fieldName, JSON.stringify(fieldValue));
          index += 2;
        } else {
          debug('Not supported update', fieldName, fieldValue);
          return Promise.reject(new Error('update error'));
        }
      });

      var where = buildWhereClause({ schema: schema, index: index, query: query });
      values.push.apply(values, _toConsumableArray(where.values));

      var qs = 'UPDATE ":name1:" SET ' + updatePatterns.join(',') + ' WHERE ' + where.pattern;
      var sqlText = getSqlTextByArray(qs, values);
      return this.conn.execute(sqlText).then(function (result) {
        if (result.rowsAffected > 0) {
          return _this15.find(className, schema, query, { limit: result.rowsAffected });
        }
        return Promise.resolve();
      }).then(function (updateObjects) {
        if (updateObjects.length >= 1) {
          return updateObjects[0];
        }
        return updateObjects;
      });
    }
  }, {
    key: 'findOneAndUpdate',
    value: function findOneAndUpdate(className, schema, query, update) {
      debug('findOneAndUpdate', className, query, update);
      return this.updateObjectsByQuery(className, schema, query, update);
    }
  }, {
    key: 'upsertOneObject',
    value: function upsertOneObject(className, schema, query, update) {
      debug('upsertOneObject', { className: className, query: query, update: update });

      var createValue = Object.assign({}, query, update);
      return this.createObject(className, schema, createValue).catch(function (error) {
        throw error;
      });
    }
  }, {
    key: 'find',
    value: function find(className, schema, query, _ref2) {
      var skip = _ref2.skip,
          limit = _ref2.limit,
          sort = _ref2.sort,
          keys = _ref2.keys;

      debug('find', className, query, { skip: skip, limit: limit, sort: sort, keys: keys });

      var hasLimit = limit !== undefined;
      var hasSkip = skip !== undefined;
      var values = [className];
      var where = buildWhereClause({ schema: schema, query: query, index: 2 });
      values.push.apply(values, _toConsumableArray(where.values));

      var wherePattern = where.pattern.length > 0 ? 'WHERE ' + where.pattern : '';
      // 这里把limit，offset改为rownum分页，分四种情况
      // 1、没有limit，offset，rowstart=0，rowend=0
      // 2、有limit，没有offset，rowstart=0，rowend=limit
      // 3、没有limit，有offset，rowstart=skip，rowend=0
      // 4、有limit，有offset，rowstart=skip，rowend=limit+skip
      var rowStart = 0;
      var rowEnd = 0;
      if (hasLimit) rowEnd = limit;
      if (hasSkip) {
        rowStart = skip;
        if (rowEnd > 0) {
          rowEnd += skip;
        }
      }
      var sortPattern = '';
      if (sort) {
        var sorting = [];
        Object.keys(sort).forEach(function (key) {
          // Using $idx pattern gives:  non-integer constant in ORDER BY
          // 这里先不处理User表的password排序
          if (className === '_User' && key === 'password') return;
          // 判断排序的列是否被删除了
          if (Object.keys(schema.fields).indexOf(key) < 0) return;
          if (sort[key] === 1) {
            sorting.push('"' + key + '" ASC');
          }
          sorting.push('"' + key + '" DESC');
        });
        sorting = sorting.join(',');
        sortPattern = sort !== undefined && Object.keys(sort).length > 0 && sorting.length ? 'ORDER BY ' + sorting : '';
      }
      if (where.sorts && Object.keys(where.sorts).length > 0) {
        sortPattern = 'ORDER BY ' + where.sorts.join(',');
      }

      var columns = '*';
      if (keys) {
        // TODO 这里的$score使用MATCH和AGAISNST做聚合，oracle不支持
        // Exclude empty keys
        // keys = keys.filter(key => key.length > 0);
        // columns = keys.map((key, index) => {
        //   if (key === '$score') {
        //     return '*, MATCH (`$2:name`) AGAINST (\'$3:name\') as score';
        //   }
        //   return `\:name${index + values.length + 1}:`;
        // }).join(',');
        // values = values.concat(keys);
      }

      var sq = 'SELECT * FROM ":name1:" ' + wherePattern + ' ' + sortPattern;
      var norsql = getSqlTextByArray(sq.trim(), values);

      var qs = 'SELECT * FORM (SELECT st.' + columns + ', ROWNUM rn FROM (' + norsql + ') st WHERE ROWNUM <= :rowEnd:) WHERE rn > :rowStart:';
      var sqlText = '';

      if (rowStart === 0 && rowEnd === 0) {
        sqlText = norsql;
      } else if (rowStart > 0 && rowEnd === 0) {
        sqlText = qs.replace(':rowEnd:', '(SELECT COUNT(*) FROM ' + className + ')').replace(':rowStart:', skip);
      } else if (rowStart === 0 && rowEnd > 0) {
        sqlText = 'SELECT ' + columns + ' FROM (' + norsql + ') WHERE ROWNUM < ' + limit;
      } else {
        sqlText = qs.replace(':rowEnd:', limit).replace(':rowStart:', skip);
      }

      return this.conn.execute(sqlText, {}, { outFormat: oracledb.OBJECT }).then(function (result) {
        return result.rows.map(function (obj) {
          var object = obj;
          Object.keys(schema.fields).forEach(function (fieldName) {
            if (schema.fields[fieldName].type === 'Pointer' && object[fieldName]) {
              object[fieldName] = { objectId: object[fieldName], __type: 'Pointer', className: schema.fields[fieldName].targetClass };
            }
            if (schema.fields[fieldName].type === 'Relation') {
              object[fieldName] = {
                __type: 'Relation',
                className: schema.fields[fieldName].targetClass
              };
            }
            if (object[fieldName] && schema.fields[fieldName].type === 'GeoPoint') {
              object[fieldName] = {
                __type: 'GeoPoint',
                latitude: object[fieldName].y,
                longitude: object[fieldName].x
              };
            }
            if (object[fieldName] && schema.fields[fieldName].type === 'File') {
              object[fieldName] = {
                __type: 'File',
                name: object[fieldName]
              };
            }
            if (object[fieldName] !== undefined && schema.fields[fieldName].type === 'Boolean') {
              object[fieldName] = object[fieldName] === 1;
            }
            if (object[fieldName] && (schema.fields[fieldName].type === 'Object' || schema.fields[fieldName].type === 'Array')) {
              object[fieldName] = JSON.parse(object[fieldName]);
            }
          });

          if (object.createdAt) {
            object.createdAt = object.createdAt.toISOString();
          }
          if (object.updatedAt) {
            object.updatedAt = object.updatedAt.toISOString();
          }

          Object.keys(object).forEach(function (key) {
            if (object[key] === null) {
              delete object[key];
            }
            if (object[key] instanceof Date) {
              object[key] = { __type: 'Date', iso: object[key].toISOString() };
            }
          });
          return object;
        });
      });
    }
  }, {
    key: 'ensureUniqueness',
    value: function ensureUniqueness(className, schema, fieldNames) {
      debug('ensureUniqueness', className, schema, fieldNames);

      var constraintName = 'unique_' + fieldNames.sort().join('_');
      var constraintPatterns = fieldNames.map(function (fieldName, index) {
        return ':name' + (index + 3) + ':';
      });
      var qs = 'ALTER TABLE ":name1:" ADD CONSTRAINT ":name2:" UNIQUE ("' + constraintPatterns.join('", "') + '")';

      var sqlText = getSqlTextByArray(qs, [className, constraintName].concat(_toConsumableArray(fieldNames)));

      return this.conn.execute(sqlText).catch(function (error) {
        // 2261，数据库中存在此唯一
        if (error.errorNum === 2261) return Promise.resolve();
        throw error;
      });
    }
  }, {
    key: 'count',
    value: function count(className, schema, query) {
      var _this16 = this;

      debug('count', { className: className, query: query });

      var values = [className];
      var where = buildWhereClause({ schema: schema, query: query, index: 2 });
      values.push.apply(values, _toConsumableArray(where.values));

      var wherePattern = where.pattern.length > 0 ? 'WHERE ' + where.pattern : '';
      var qs = 'SELECT count(*) FROM ":name1:" ' + wherePattern;
      var sqlText = getSqlTextByArray(qs, values).trim();
      return this.connect().then(function () {
        return _this16.conn.execute(sqlText);
      }).then(function (res) {
        return res.rows[0][0];
      }).catch(function (error) {
        throw error;
      });
    }
    // 去重

  }, {
    key: 'distinct',
    value: function distinct(className, schema, query, fieldName) {
      debug('distinct', className, schema, query, fieldName);
      return Promise.resolve(null);
    }
    // 聚合

  }, {
    key: 'aggregate',
    value: function aggregate(className, schema, pipeline, readPreference) {
      debug('aggregate', className, schema, pipeline, readPreference);
      return Promise.resolve(null);
    }
  }, {
    key: 'performInitialization',
    value: function performInitialization(_ref3) {
      var _this17 = this;

      var VolatileClassesSchemas = _ref3.VolatileClassesSchemas;

      debug('performInitialization', VolatileClassesSchemas.map(function (row) {
        return row.className;
      }));
      global.isInitialized = true;

      var promises = VolatileClassesSchemas.map(function (schema) {
        return _this17.createTable(schema.className, schema);
      });

      return Promise.all(promises).then(function () {
        global.isInitialized = false;
        debug('initializationDone');
      });
    }
    // Indexing

  }, {
    key: 'createIndexes',
    value: function createIndexes(className, indexes) {
      var _this18 = this;

      debug('createIndexes', { className: className, indexes: indexes });

      var promises = indexes.map(function (i) {
        var sqlText = 'CREATE INDEX "' + i.name + '" ON "' + className + '" ("' + i.key + '")';
        return _this18.conn.execute(sqlText);
      });

      return this.connect().then(function () {
        return Promise.all(promises);
      });
    }
  }, {
    key: 'getIndexes',
    value: function getIndexes(className) {
      var _this19 = this;

      debug('getIndexes', className);

      var sqlText = 'SELECT owner, index_name, table_name FROM all_indexes WHERE table_name = \'' + className + '\'';
      return this.connect().then(function () {
        return _this19.conn.execute(sqlText, {}, { outFormat: oracledb.OBJECT });
      });
    }
  }, {
    key: 'updateSchemaWithIndexes',
    value: function updateSchemaWithIndexes() {
      debug('updateSchemaWithIndexes');
      return Promise.resolve();
    }
  }, {
    key: 'setIndexesWithSchemaFormat',
    value: function setIndexesWithSchemaFormat(className, submittedIndexes, existingIndexes, fields) {
      debug('setIndexesWithSchemaFormat', className, submittedIndexes, existingIndexes, fields);
      return Promise.resolve();
    }
  }]);

  return Adapter;
}();

module.exports = Adapter;
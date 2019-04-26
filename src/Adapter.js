const oracledb = require('oracledb');
const { defaultUniqueKeyLength } = require('./config');
const { debug, getSqlTextByArray, getDatabaseOptionsFromURI } = require('./util');
const { toParseSchema, toOracleSchema, handleDotFields, validateKeys, formatDateToOracle, toOracleValue, parseTypeToOracleType, buildWhereClause, joinTablesForSchema } = require('./format');

const TABLE_OWNER = 'MORIA';
oracledb.autoCommit = true;

class Adapter {
  constructor({ uri, collectionPrefix = '', databaseOptions = {} }) {
    this._uri = uri;
    this._collectionPrefix = collectionPrefix;

    let dbOptions = {};
    const options = databaseOptions || {};
    if (uri) {
      dbOptions = getDatabaseOptionsFromURI(uri);
      dbOptions.connectionString = `${dbOptions.host}:${dbOptions.port}/${dbOptions.database}`;
    }
    Object.keys(options).forEach((key) => {
      dbOptions[key] = options[key];
    });

    dbOptions.multipleStatements = true;
    this._databaseOptions = dbOptions;
    this.canSortOnJoinTables = false;
  }

  // 连接数据库
  connect() {
    if (this.connectionPromise) {
      return this.connectionPromise;
    }
    this.connectionPromise = oracledb.getConnection(this._databaseOptions).then((conn) => {
      this.conn = conn;
    }).catch((error) => {
      delete this.connectionPromise;
      return Promise.reject(error);
    });
    return this.connectionPromise;
  }

  // 结束关闭连接
  handleShutdown() {
    if (!this.conn) {
      return;
    }
    this.conn.close();
  }

  // 确认schema表是否存在
  _ensureSchemaCollectionExists() {
    debug('_ensureSchemaCollectionExists');

    const sqlText = 'CREATE TABLE "_SCHEMA" ("className" VARCHAR2(120 BYTE) NOT NULL, PRIMARY KEY("className"), "schema" VARCHAR2(4000), "isParseClass" NUMBER(1))';
    return this.classExists('_SCHEMA')
      .then(es => (!es ? this.conn.execute(sqlText) : Promise.resolve()));
  }

  // 创建表
  createTable(className, schema) {
    debug('createTable', className, schema);

    // 自动更新时间字段的触发器
    // const triggerSql = `CREATE OR REPLACE trigger "${className}_TR"
    //                         BEFORE INSERT OR UPDATE ON "${className}" FOR EACH ROW
    //                         BEGIN
    //                           IF UPDATING THEN
    //                             :NEW.updatedAt := CURRENT_TIMESTAMP(6);
    //                           END IF;
    //                         END;`;

    const valuesArray = [];
    const patternsArray = [];
    const fields = Object.assign({}, schema.fields);
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
    let index = 2;
    const relations = [];
    Object.keys(fields).forEach((fieldName) => {
      const parseType = fields[fieldName];
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
          patternsArray.push(`":varn${index}:" :varn${index + 1}: DEFAULT CURRENT_TIMESTAMP(6)`);
        } else if (fieldName === 'updatedAt') {
          patternsArray.push(`":varn${index}:" :varn${index + 1}: DEFAULT CURRENT_TIMESTAMP(6)`);
        } else {
          patternsArray.push(`":varn${index}:" :varn${index + 1}: NULL`);
        }
        index += 2;
        return;
      }
      patternsArray.push(`":varn${index}:" :varn${index + 1}:`);
      if (fieldName === 'objectId') {
        valuesArray.push(defaultUniqueKeyLength);
        patternsArray.push(`PRIMARY KEY (":varn${index}:")`);
      } else if (fieldName === 'email' || fieldName === 'username' || fieldName === 'name') {
        valuesArray.push(defaultUniqueKeyLength);
      } else {
        valuesArray.push(parseTypeToOracleType(parseType));
      }
      index += 2;
    });

    const qs = `CREATE TABLE ":varn1:" (${patternsArray.join(',')})`;
    const sqlText = getSqlTextByArray(qs, [className, ...valuesArray], 'varn');

    return this.connect()
      .then(() => this.classExists(className))
      .then((existsTable) => {
        if (!existsTable) {
          // TODO 自动更新时间字段
          // if (Object.keys(fields).indexOf('updatedAt') > -1) {
          //   await this.conn.execute(triggerSql);
          // }
          return this.conn.execute(sqlText);
        }
      })
      .then(() => {
        const promises = relations.map((filedName) => {
          const joinTableName = `_Join:${filedName}:${className}`;
          const joinTableSql = `CREATE TABLE "${joinTableName}" ("relatedId" varChar(120), "owningId" varChar(120), PRIMARY KEY("relatedId", "owningId"))`;
          return this.classExists(joinTableName)
            .then(etj => (!etj ? this.conn.execute(joinTableSql) : Promise.resolve()));
        });
        return Promise.all(promises);
      })
      .catch(err => console.log(err));
  }

  // 获取JSON类型的字段内容
  _getJSONSelectQuery(className, fieldName, query) {
    let wherePattern = '';
    Object.keys(query).forEach((field, index) => {
      if (index === 0) {
        wherePattern = 'WHERE';
      } else {
        wherePattern += ' AND';
      }
      wherePattern += ` "${field}" = '${query[field]}'`;
    });
    return `SELECT "${fieldName}" FROM "${className}" ${wherePattern}`.trim();
  }
  getJSONValue(className, fieldName, query) {
    debug('getJSONValue', className, fieldName, query);

    const sqlText = this._getJSONSelectQuery(className, fieldName, query);
    return this.connect()
      .then(() => this.conn.execute(sqlText))
      .then(res => (res.rows[0] ? JSON.parse(res.rows[0]) : {}));
  }
  getJSONValues(className, fieldName, query) {
    debug('getJSONValues', className, fieldName, query);

    const sqlText = this._getJSONSelectQuery(className, fieldName, query);
    return this.connect()
      .then(() => this.conn.execute(sqlText))
      .then((res) => {
        const result = [];
        res.rows.forEach((row) => {
          result.push(row ? JSON.parse(row[fieldName]) : '');
        });
        return result;
      });
  }

  // 判断表是否存在
  classExists(className, owner = TABLE_OWNER) {
    debug('classExists', className);
    const sql = `select count(1) from all_tables where TABLE_NAME = '${className}' and OWNER='${owner}'`;
    return this.connect()
      .then(() => this.conn.execute(sql))
      .then(result => !!result.rows[0][0])
      .catch(() => Promise.resolve(false));
  }
  setClassLevelPermissions(className, clps) {
    debug('setClassLevelPermissions', className);

    return this._ensureSchemaCollectionExists()
      .then(() => this.getJSONValue(className, 'schema', { className }))
      .then((res) => {
        res.classLevelPermissions = clps;
        const sqlText = `UPDATE "_SCHEMA" SET "schema" = '${JSON.stringify(res)}' WHERE "className" = '${className}'`;
        return this.conn.execute(sqlText);
      });
  }
  createClass(className, schema) {
    debug('createClass', { className, schema });

    const qs = `INSERT INTO "_SCHEMA" ("className", "schema", "isParseClass") VALUES ('${schema.className}', '${JSON.stringify(schema)}', 1)`;
    return this.createTable(className, schema)
      .then(() => this.conn.execute(qs))
      .then(() => toParseSchema(schema))
      .catch((err) => {
        throw err;
      });
  }
  addFieldIfNotExists(className, fieldName, type) {
    debug('addFieldIfNotExists', { className, fieldName, type });

    let promise = Promise.resolve();
    if (type.type !== 'Relation') {
      let oracleType = parseTypeToOracleType(type);
      if (type.type === 'Date') {
        oracleType = 'timestamp(6) null default null';
      }
      promise = this.conn.execute(`ALTER TABLE "${className}" ADD ("${fieldName}" ${oracleType})`);
    } else {
      const relationTable = `_Join:${fieldName}:${className}`;
      const sq = `CREATE TABLE "${relationTable}" ("relatedId" varChar2(120), "owningId" varChar2(120), PRIMARY KEY("relatedId", "owningId"))`;
      promise = this.classExists(relationTable)
        .then(et => (!et ? this.conn.execute(sq) : Promise.resolve()));
    }

    return promise
      .then(() => {
        // 先查询出当前类的内容
        const schemaQuerySql = `SELECT "schema" FROM "_SCHEMA" WHERE "className" = '${className}'`;
        return this.conn.execute(schemaQuerySql, {}, { outFormat: oracledb.OBJECT });
      })
      .then((schemaResult) => {
        const schemaObject = schemaResult.rows[0].schema ? JSON.parse(schemaResult.rows[0].schema) : { fields: {} };
        if (!schemaObject.fields[fieldName]) {
          schemaObject.fields[fieldName] = type;
          const updateSchemaSql = `UPDATE "_SCHEMA" SET "schema"= '${JSON.stringify(schemaObject)}' WHERE "className"='${className}'`;
          return this.conn.execute(updateSchemaSql);
        }
        throw 'Attempted to add a field that already exists';
      });
  }
  deleteClass(className) {
    debug('deleteClass', className);

    const dropText = `DROP TABLE "${className}"`;
    const deleteText = `DELETE FROM "_SCHEMA" WHERE "className" = '${className}';`;
    return this.connect()
      .then(() => this.classExists(className))
      .then(et => (!et ? this.conn.execute(dropText) : Promise.resolve()))
      .then(() => this.conn.execute(deleteText))
      .catch((error) => {
        throw error;
      });
  }
  deleteAllClasses(fast) {
    debug('deleteAllClasses', fast);

    const now = new Date().getTime();
    const selectText = 'SELECT * FROM "_SCHEMA"';
    const originClass = ['_SCHEMA', '_PushStatus', '_JobStatus', '_JobSchedule', '_Hooks', '_GlobalConfig', '_Audience'];
    return this.connect()
      .then(() => this.conn.execute(selectText, {}, { outFormat: oracledb.OBJECT }))
      .then((res) => {
        const resClass = res.rows.map(r => r.className);
        const joins = res.rows.reduce((list, schema) => list.concat(joinTablesForSchema(JSON.parse(schema.schema))), []);
        const classes = [...originClass, ...resClass, ...joins];
        const promises = classes.map((c) => {
          const sqlText = `DROP TABLE "${c}"`;
          return this.classExists(c)
            .then(et => (et ? this.conn.execute(sqlText) : Promise.resolve()));
        });
        return Promise.all(promises);
      })
      .then(() => {
        debug(`deleteAllClasses done in ${new Date().getTime() - now}`);
      })
      .catch((error) => {
        throw error;
      });
  }
  deleteFields(className, schema, fieldNames) {
    debug('deleteFields', className, schema, fieldNames);

    const fields = fieldNames.reduce((list, fieldName) => {
      const field = schema.fields[fieldName];
      if (field.type !== 'Relation') {
        list.push(fieldName);
      }
      delete schema.fields[fieldName];
      return list;
    }, []);

    const values = [className, ...fields];
    const columns = fields.map((name, idx) => `":name${idx + 2}:"`).join(', DROP COLUMN');
    // 这里先取出JSON字段做处理，然后再更改表内容
    return this.getJSONValue('_SCHEMA', 'schema', { className })
      .then((res) => {
        res.fields = schema.fields;
        const updateSql = `UPDATE "_SCHEMA" SET "schema" = '${JSON.stringify(res)}' WHERE "className" = '${className}'`;
        return this.conn.execute(updateSql);
      })
      .then(() => {
        if (values.length > 1) {
          const aqs = `ALTER TABLE ":name1:" DROP COLUMN ${columns}`;
          const alterText = getSqlTextByArray(aqs, values);
          return this.conn.execute(alterText);
        }
        return Promise.resolve();
      })
      .catch((error) => {
        throw error;
      });
  }
  getAllClasses() {
    debug('getAllClasses');

    return this._ensureSchemaCollectionExists()
      .then(() => this.conn.execute('SELECT * FROM "_SCHEMA"', {}, { outFormat: oracledb.OBJECT }))
      .then(result => result.rows.map(row => toParseSchema({ className: row.className, ...JSON.parse(row.schema) })));
  }
  getClass(className) {
    debug('getClass', className);

    // TODO 原来有按字符排序的sql：SELECT * FROM `_SCHEMA` WHERE `className` COLLATE latin1_general_cs =\'$1:name\
    const classSql = `SELECT * FROM "_SCHEMA" WHERE "className" = '${className}'`;
    return this.conn.execute(classSql, {}, { outFormat: oracledb.OBJECT })
      .then((result) => {
        if (result.rows.length === 1) {
          return JSON.parse(result.rows[0].schema);
        }
        throw undefined;
      })
      .then(toParseSchema);
  }
  createObject(className, schema, object) {
    debug('createObject', className, object);

    let columnsArray = [];
    const valuesArray = [];
    schema = toOracleSchema(schema);
    const geoPoints = {};
    object = handleDotFields(object);
    validateKeys(object);

    Object.keys(object).forEach((fieldName) => {
      if (object[fieldName] === null) {
        return;
      }
      const authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
      if (authDataMatch) {
        const provider = authDataMatch[1];
        object.authData = object.authData || {};
        object.authData[provider] = object[fieldName];
        delete object[fieldName];
        fieldName = 'authData';
      }

      columnsArray.push(fieldName);
      if (!schema.fields[fieldName] && className === '_User') {
        if (fieldName === '_email_verify_token' ||
          fieldName === '_failed_login_count' ||
          fieldName === '_perishable_token' ||
          fieldName === '_password_history') {
          valuesArray.push(object[fieldName]);
        }
        if (fieldName === '_account_lockout_expires_at' ||
          fieldName === '_perishable_token_expires_at' ||
          fieldName === '_password_changed_at' ||
          fieldName === '_email_verify_token_expires_at') {
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
          throw `Type ${schema.fields[fieldName].type} not supported yet`;
      }
    });

    columnsArray = columnsArray.concat(Object.keys(geoPoints));
    const initialValues = valuesArray.map((val, index) => {
      const fieldName = columnsArray[index];
      if ((schema.fields[fieldName] && (schema.fields[fieldName].type === 'Boolean' || schema.fields[fieldName].type === 'Date')) || val === null) {
        return `:name${index + 2 + columnsArray.length}:`;
      }

      return `':name${index + 2 + columnsArray.length}:'`;
    });
    const geoPointsInjects = Object.keys(geoPoints).map((key) => {
      const value = geoPoints[key];
      valuesArray.push(value.longitude, value.latitude);
      const l = valuesArray.length + columnsArray.length;
      return `POINT(${l}, ${l + 1})`;
    });

    const columnsPattern = columnsArray.map((col, index) => `":name${index + 2}:"`).join(',');
    const valuesPattern = initialValues.concat(geoPointsInjects).join(',');
    const qs = `INSERT INTO ":name1:" (${columnsPattern}) VALUES (${valuesPattern})`;
    const values = [className, ...columnsArray, ...valuesArray];
    const sqlText = getSqlTextByArray(qs, values);

    return this.conn.execute(sqlText)
      .then(() => ({ ops: [object] }));
  }
  deleteObjectsByQuery(className, schema, query) {
    debug('deleteObjectsByQuery', className, query);

    const values = [className];
    const index = 2;
    const where = buildWhereClause({ schema, index, query });
    values.push(...where.values);
    if (Object.keys(query).length === 0) {
      where.pattern = 'TRUE';
    }
    const qs = `DELETE FROM ":name1:" WHERE ${where.pattern}`;
    const sqlText = getSqlTextByArray(qs, values);

    return this.connect()
      .then(() => this.conn.execute(sqlText))
      .then((res) => {
        if (res.rowsAffected === 0) {
          throw 'Object not found.';
        } else {
          return res.rowsAffected;
        }
      });
  }
  updateObjectsByQuery(className, schema, query, update) {
    debug('updateObjectsByQuery', className, query, update);

    const updatePatterns = [];
    const values = [className];
    let index = 2;
    schema = toOracleSchema(schema);

    // const originalUpdate = { ...update };
    update = handleDotFields(update);
    // Resolve authData first,
    // So we don't end up with multiple key updates
    Object.keys(update).forEach((fieldName) => {
      const authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
      if (authDataMatch) {
        const provider = authDataMatch[1];
        const value = update[fieldName];
        delete update[fieldName];
        update.authData = update.authData || {};
        update.authData[provider] = value;
      }
    });

    Object.keys(update).forEach((fieldName) => {
      const fieldValue = update[fieldName];
      if (fieldValue === null) {
        updatePatterns.push(`:name${index}: = NULL`);
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
        updatePatterns.push(`":name${index}:" = COALESCE(":name${index}:", 0) + :name${index + 1}:`);
        values.push(fieldName, fieldValue.amount);
        index += 2;
      } else if (fieldValue.__op === 'Add') {
        updatePatterns.push(`":name${index}:"= JSON_ARRAY_INSERT(COALESCE(":name${index}:", '[]'), CONCAT('$[',JSON_LENGTH(":name${index}:"),']'), ':name${index + 1}:')`);
        values.push(fieldName, JSON.stringify(fieldValue.objects));
        index += 2;
      } else if (fieldValue.__op === 'Delete') {
        updatePatterns.push(`":name${index}:" = $${index + 1}`);
        values.push(fieldName, null);
        index += 2;
      } else if (fieldValue.__op === 'Remove') {
        fieldValue.objects.forEach((obj) => {
          updatePatterns.push(`":name${index}:" = JSON_REMOVE(":name${index}:", REPLACE(JSON_SEARCH(COALESCE(":name${index}:",'[]'), 'one', ':name${index + 1}:'),'"',''))`);
          if (typeof obj === 'object') {
            values.push(fieldName, JSON.stringify(obj));
          } else {
            values.push(fieldName, obj);
          }
          index += 2;
        });
      } else if (fieldValue.__op === 'AddUnique') {
        fieldValue.objects.forEach((obj) => {
          updatePatterns.push(`":name${index}:" = if (JSON_CONTAINS(":name${index}:", ':name${index + 1}:') = 0, JSON_MERGE(":name${index}:",':name${index + 1}:'),":name${index}:")`);
          if (typeof obj === 'object') {
            values.push(fieldName, JSON.stringify(obj));
          } else {
            values.push(fieldName, obj);
          }
          index += 2;
        });
      } else if (fieldName === 'updatedAt' || fieldName === 'finishedAt') { // TODO: stop special casing this. It should check for __type === 'Date' and use .iso
        updatePatterns.push(`":name${index}:" = :name${index + 1}:`);
        values.push(fieldName, formatDateToOracle(fieldValue));
        index += 2;
      } else if (typeof fieldValue === 'string') {
        updatePatterns.push(`":name${index}:" = ':name${index + 1}:'`);
        values.push(fieldName, fieldValue);
        index += 2;
      } else if (typeof fieldValue === 'boolean') {
        updatePatterns.push(`":name${index}:" = :name${index + 1}:`);
        values.push(fieldName, fieldValue);
        index += 2;
      } else if (fieldValue.__type === 'Pointer') {
        updatePatterns.push(`":name${index}:" = ':name${index + 1}:'`);
        values.push(fieldName, fieldValue.objectId);
        index += 2;
      } else if (fieldValue.__type === 'Date') {
        updatePatterns.push(`":name${index}:" = :name${index + 1}:`);
        values.push(fieldName, toOracleValue(fieldValue));
        index += 2;
      } else if (fieldValue instanceof Date) {
        updatePatterns.push(`":name${index}:" = ':name${index + 1}:'`);
        values.push(fieldName, fieldValue);
        index += 2;
      } else if (fieldValue.__type === 'File') {
        updatePatterns.push(`":name${index}:" = ':name${index + 1}:'`);
        values.push(fieldName, toOracleValue(fieldValue));
        index += 2;
      } else if (fieldValue.__type === 'GeoPoint') {
        updatePatterns.push(`":name${index}:" = POINT($${index + 1}, $${index + 2})`);
        values.push(fieldName, fieldValue.longitude, fieldValue.latitude);
        index += 3;
      } else if (fieldValue.__type === 'Relation') {
        // noop
      } else if (typeof fieldValue === 'number') {
        updatePatterns.push(`":name${index}:" = $${index + 1}`);
        values.push(fieldName, fieldValue);
        index += 2;
      } else if (typeof fieldValue === 'object'
        && schema.fields[fieldName]
        && schema.fields[fieldName].type === 'Object') {
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

        updatePatterns.push(`":name${index}:" = ':name${index + 1}:'`);
        values.push(fieldName, JSON.stringify(fieldValue));
        index += 2;
      } else if (Array.isArray(fieldValue)
        && schema.fields[fieldName]
        && schema.fields[fieldName].type === 'Array') {
        updatePatterns.push(`":name${index}:" = ':name${index + 1}:'`);
        values.push(fieldName, JSON.stringify(fieldValue));
        index += 2;
      } else {
        debug('Not supported update', fieldName, fieldValue);
        return Promise.reject(new Error('update error'));
      }
    });

    const where = buildWhereClause({ schema, index, query });
    values.push(...where.values);

    const qs = `UPDATE ":name1:" SET ${updatePatterns.join(',')} WHERE ${where.pattern}`;
    const sqlText = getSqlTextByArray(qs, values);
    return this.conn.execute(sqlText)
      .then((result) => {
        if (result.rowsAffected > 0) {
          return this.find(className, schema, query, { limit: result.rowsAffected });
        }
        return Promise.resolve();
      })
      .then((updateObjects) => {
        if (updateObjects.length >= 1) {
          return updateObjects[0];
        }
        return updateObjects;
      });
  }
  findOneAndUpdate(className, schema, query, update) {
    debug('findOneAndUpdate', className, query, update);
    return this.updateObjectsByQuery(className, schema, query, update);
  }
  upsertOneObject(className, schema, query, update) {
    debug('upsertOneObject', { className, query, update });

    const createValue = Object.assign({}, query, update);
    return this.createObject(className, schema, createValue)
      .catch((error) => {
        throw error;
      });
  }
  find(className, schema, query, { skip, limit, sort, keys }) {
    debug('find', className, query, { skip, limit, sort, keys });

    const hasLimit = limit !== undefined;
    const hasSkip = skip !== undefined;
    const values = [className];
    const where = buildWhereClause({ schema, query, index: 2 });
    values.push(...where.values);

    const wherePattern = where.pattern.length > 0 ? `WHERE ${where.pattern}` : '';
    // 这里把limit，offset改为rownum分页，分四种情况
    // 1、没有limit，offset，rowstart=0，rowend=0
    // 2、有limit，没有offset，rowstart=0，rowend=limit
    // 3、没有limit，有offset，rowstart=skip，rowend=0
    // 4、有limit，有offset，rowstart=skip，rowend=limit+skip
    let rowStart = 0;
    let rowEnd = 0;
    if (hasLimit) rowEnd = limit;
    if (hasSkip) {
      rowStart = skip;
      if (rowEnd > 0) {
        rowEnd += skip;
      }
    }
    let sortPattern = '';
    if (sort) {
      let sorting = [];
      Object.keys(sort).forEach((key) => {
        // Using $idx pattern gives:  non-integer constant in ORDER BY
        // 这里先不处理User表的password排序
        if (className === '_User' && key === 'password') return;
        // 判断排序的列是否被删除了
        if (Object.keys(schema.fields).indexOf(key) < 0) return;
        if (sort[key] === 1) {
          sorting.push(`"${key}" ASC`);
        }
        sorting.push(`"${key}" DESC`);
      });
      sorting = sorting.join(',');
      sortPattern = sort !== undefined && Object.keys(sort).length > 0 && sorting.length ? `ORDER BY ${sorting}` : '';
    }
    if (where.sorts && Object.keys(where.sorts).length > 0) {
      sortPattern = `ORDER BY ${where.sorts.join(',')}`;
    }

    const columns = '*';
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

    const sq = `SELECT * FROM ":name1:" ${wherePattern} ${sortPattern}`;
    const norsql = getSqlTextByArray(sq.trim(), values);

    const qs = `SELECT * FORM (SELECT st.${columns}, ROWNUM rn FROM (${norsql}) st WHERE ROWNUM <= :rowEnd:) WHERE rn > :rowStart:`;
    let sqlText = '';

    if (rowStart === 0 && rowEnd === 0) {
      sqlText = norsql;
    } else if (rowStart > 0 && rowEnd === 0) {
      sqlText = qs.replace(':rowEnd:', `(SELECT COUNT(*) FROM ${className})`).replace(':rowStart:', skip);
    } else if (rowStart === 0 && rowEnd > 0) {
      sqlText = `SELECT ${columns} FROM (${norsql}) WHERE ROWNUM < ${limit}`;
    } else {
      sqlText = qs.replace(':rowEnd:', limit).replace(':rowStart:', skip);
    }

    return this.conn.execute(sqlText, {}, { outFormat: oracledb.OBJECT })
      .then(result => result.rows.map((obj) => {
        const object = obj;
        Object.keys(schema.fields).forEach((fieldName) => {
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

        Object.keys(object).forEach((key) => {
          if (object[key] === null) {
            delete object[key];
          }
          if (object[key] instanceof Date) {
            object[key] = { __type: 'Date', iso: object[key].toISOString() };
          }
        });
        return object;
      }));
  }
  ensureUniqueness(className, schema, fieldNames) {
    debug('ensureUniqueness', className, schema, fieldNames);

    const constraintName = `unique_${fieldNames.sort().join('_')}`;
    const constraintPatterns = fieldNames.map((fieldName, index) => `:name${index + 3}:`);
    const qs = `ALTER TABLE ":name1:" ADD CONSTRAINT ":name2:" UNIQUE ("${constraintPatterns.join('", "')}")`;

    const sqlText = getSqlTextByArray(qs, [className, constraintName, ...fieldNames]);

    return this.conn.execute(sqlText)
      .catch((error) => {
        // 2261，数据库中存在此唯一
        if (error.errorNum === 2261) return Promise.resolve();
        throw error;
      });
  }
  count(className, schema, query) {
    debug('count', { className, query });

    const values = [className];
    const where = buildWhereClause({ schema, query, index: 2 });
    values.push(...where.values);

    const wherePattern = where.pattern.length > 0 ? `WHERE ${where.pattern}` : '';
    const qs = `SELECT count(*) FROM ":name1:" ${wherePattern}`;
    const sqlText = getSqlTextByArray(qs, values).trim();
    return this.connect()
      .then(() => this.conn.execute(sqlText))
      .then(res => res.rows[0][0])
      .catch((error) => {
        throw error;
      });
  }
  // 去重
  distinct(className, schema, query, fieldName) {
    debug('distinct', className, schema, query, fieldName);
    return Promise.resolve(null);
  }
  // 聚合
  aggregate(className, schema, pipeline, readPreference) {
    debug('aggregate', className, schema, pipeline, readPreference);
    return Promise.resolve(null);
  }
  performInitialization({ VolatileClassesSchemas }) {
    debug('performInitialization', VolatileClassesSchemas.map(row => row.className));
    global.isInitialized = true;

    const promises = VolatileClassesSchemas.map(schema => this.createTable(schema.className, schema));

    return Promise.all(promises)
      .then(() => {
        global.isInitialized = false;
        debug('initializationDone');
      });
  }
  // Indexing
  createIndexes(className, indexes) {
    debug('createIndexes', { className, indexes });

    const promises = indexes.map((i) => {
      const sqlText = `CREATE INDEX "${i.name}" ON "${className}" ("${i.key}")`;
      return this.conn.execute(sqlText);
    });

    return this.connect()
      .then(() => Promise.all(promises));
  }
  getIndexes(className) {
    debug('getIndexes', className);

    const sqlText = `SELECT owner, index_name, table_name FROM all_indexes WHERE table_name = '${className}'`;
    return this.connect()
      .then(() => this.conn.execute(sqlText, {}, { outFormat: oracledb.OBJECT }));
  }
  updateSchemaWithIndexes() {
    debug('updateSchemaWithIndexes');
    return Promise.resolve();
  }
  setIndexesWithSchemaFormat(className, submittedIndexes, existingIndexes, fields) {
    debug('setIndexesWithSchemaFormat', className, submittedIndexes, existingIndexes, fields);
    return Promise.resolve();
  }
}

module.exports = Adapter;

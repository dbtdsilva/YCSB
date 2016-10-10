/**
 * Copyright (c) 2013-2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 *
 */
/**
 * LMDB client binding for YCSB.
 *
 * Created by Diogo Silva.
 */
package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import org.fusesource.lmdbjni.*;
import static org.fusesource.lmdbjni.Constants.*;

/**
 * LMDB client.
 *
 * See {@code lmdb/README.md} for details.
 *
 * @author dbtdsilva
 */
public class LMDBClient extends DB {

  private final Logger logger = org.apache.log4j.Logger.getLogger(getClass());
  protected static final ObjectMapper MAPPER = new ObjectMapper();
  private Env env;
  private Database db;

  @Override
  public void init() throws DBException {
    env = new Env("/tmp/mydb");
    env.setMapSize(1024 * 1024 * 512);
    db = env.openDatabase();
  }

  @Override
  public void cleanup() throws DBException {
    db.close();
    env.close();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
          HashMap<String, ByteIterator> result) {
    key = createQualifiedKey(table, key);
    try {
      String value = string(db.get(bytes(key)));
      fromJson(value, fields, result);
      return Status.OK;
    } catch (Exception ex) {
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
          Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      db.put(bytes(key), bytes(toJson(values)));
      return Status.OK;
    } catch (Exception e) {
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    key = createQualifiedKey(table, key);
    try {
      db.put(bytes(key), bytes(toJson(values)));
    } catch (IOException ex) {
      java.util.logging.Logger.getLogger(LMDBClient.class.getName()).log(Level.SEVERE, null, ex);
    }
    return Status.OK;
    /*
    try {
      
      return Status.OK;
    } catch (Exception e) {
      logger.error("Error updating value with key: " + key, e);
      return Status.ERROR;
    }*/
  }

  @Override
  public Status delete(String table, String key) {
    key = createQualifiedKey(table, key);
    db.delete(bytes(key));
    return Status.OK;
  }

  protected static String createQualifiedKey(String table, String key) {
    return MessageFormat.format("{0}-{1}", table, key);
  }

  protected static void fromJson(
          String value, Set<String> fields,
          Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    Map.Entry<String, JsonNode> jsonField;
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Map.Entry<String, JsonNode>> jsonFields = json.getFields();
            jsonFields.hasNext();/* increment in loop body */) {
      jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
          throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    HashMap<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }
}

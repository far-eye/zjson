/*
 * Copyright 2016 flipkart.com zjsonpatch.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.fareye.zjson;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

class InPlaceApplyProcessor implements JsonPatchProcessor {

    private JsonNode target;
    private EnumSet<CompatibilityFlags> flags;

    InPlaceApplyProcessor(JsonNode target) {
        this(target, CompatibilityFlags.defaults());
    }

    InPlaceApplyProcessor(JsonNode target, EnumSet<CompatibilityFlags> flags) {
        this.target = target;
        this.flags = flags;
    }

    public JsonNode result() {
        return target;
    }

    @Override
    public void move(JsonPointer fromPath, JsonPointer toPath) throws JsonPointerEvaluationException {
        JsonNode valueNode = fromPath.evaluate(target);
        remove(fromPath);
        set(toPath, valueNode, Operation.MOVE);
    }

    @Override
    public void copy(JsonPointer fromPath, JsonPointer toPath) throws JsonPointerEvaluationException {
        JsonNode valueNode = fromPath.evaluate(target);
        JsonNode valueToCopy = valueNode != null ? valueNode.deepCopy() : null;
        set(toPath, valueToCopy, Operation.COPY);
    }

    private static String show(JsonNode value) {
        if (value == null || value.isNull())
            return "null";
        else if (value.isArray())
            return "array";
        else if (value.isObject())
            return "object";
        else
            return "value " + value.toString();     // Caveat: numeric may differ from source (e.g. trailing zeros)
    }

    @Override
    public void test(JsonPointer path, JsonNode value) throws JsonPointerEvaluationException {
        JsonNode valueNode = path.evaluate(target);
        if (!valueNode.equals(value))
            throw new JsonPatchApplicationException(
                    "Expected " + show(value) + " but found " + show(valueNode), Operation.TEST, path);
    }

    @Override
    public void add(JsonPointer path, JsonNode value) throws JsonPointerEvaluationException {
        set(path, value, Operation.ADD);
    }

    @Override
    public void replace(JsonPointer path, JsonNode value) throws JsonPointerEvaluationException {
        if (path.isRoot()) {
            target = value;
            return;
        }

        JsonNode parentNode = path.getParent().evaluate(target);
        if(parentNode==null)
            return;
        JsonPointer.RefToken token = path.last();
        if (parentNode.isObject()) {
            if (!flags.contains(CompatibilityFlags.ALLOW_MISSING_TARGET_OBJECT_ON_REPLACE) &&
                    !parentNode.has(token.getField()))
                     return; // just skip the patch, instead of throwing exception
//                throw new JsonPatchApplicationException(
//                        "Missing field \"" + token.getField() + "\"", Operation.REPLACE, path.getParent());
            ((ObjectNode) parentNode).replace(token.getField(), value);
        } else if (parentNode.isArray()) {
            if (token.getIndex() >= parentNode.size())
                throw new JsonPatchApplicationException(
                        "Array index " + token.getIndex() + " out of bounds", Operation.REPLACE, path.getParent());
            ((ArrayNode) parentNode).set(token.getIndex(), value);
        } else {
            throw new JsonPatchApplicationException(
                    "Can't reference past scalar value", Operation.REPLACE, path.getParent());
        }
    }

    @Override
    public void remove(JsonPointer path) throws JsonPointerEvaluationException {
        if (path.isRoot())
            throw new JsonPatchApplicationException("Cannot remove document root", Operation.REMOVE, path);

        JsonNode parentNode = path.getParent().evaluate(target);
        JsonPointer.RefToken token = path.last();
        if (parentNode.isObject())
            ((ObjectNode) parentNode).remove(token.getField());
        else if (parentNode.isArray()) {
            if (!flags.contains(CompatibilityFlags.REMOVE_NONE_EXISTING_ARRAY_ELEMENT) &&
                    token.getIndex() >= parentNode.size())
                throw new JsonPatchApplicationException(
                        "Array index " + token.getIndex() + " out of bounds", Operation.REPLACE, path.getParent());
            ((ArrayNode) parentNode).remove(token.getIndex());
        } else {
            throw new JsonPatchApplicationException(
                    "Cannot reference past scalar value", Operation.REPLACE, path.getParent());
        }
    }



    private void set(JsonPointer path, JsonNode value, Operation forOp) throws JsonPointerEvaluationException {
        if (path.isRoot())
            target = value;
        else {
            JsonNode parentNode = path.getParent().evaluate(target);
            if(parentNode==null)
                return;
            if (!parentNode.isContainerNode())
                throw new JsonPatchApplicationException("Cannot reference past scalar value", forOp, path.getParent());
            else if (parentNode.isArray())
                addToArray(path, value, parentNode);
            else
                addToObject(path, parentNode, value);
        }
    }

    private void addToObject(JsonPointer path, JsonNode node, JsonNode value) {
        String key = path.last().getField();
        if("maxIdMap".equals(key) && node.has(key) && node.get(key).isObject() && value.isObject()){
            mergeInBaseForMaxIdMap(node.get(key), value);
        }
        else if (path.toString().contains("/maxIdMap/")){
            ObjectNode baseObjectNode = (ObjectNode) node;
            JsonNode tempValue = value;

            if(baseObjectNode.get(key)!=null && baseObjectNode.get(key).longValue() > value.longValue()){
                tempValue = baseObjectNode.get(key);
            }

            baseObjectNode.set(key,tempValue );
        }
        else if (node.has(key) && node.get(key).isObject() && value.isObject()) {
            mergeInBase(node.get(key), value);
        }
        else {
            final ObjectNode target = (ObjectNode) node;
            target.set(key, value);
        }
    }

    public void mergeInBase (JsonNode base, JsonNode value) {
        Iterator<String> fieldsIterator = value.fieldNames();

        while (fieldsIterator.hasNext()) {
            String fieldName = fieldsIterator.next();
            if (base.has(fieldName) && base.get(fieldName).isObject()) {
                mergeInBase(base.get(fieldName), value.get(fieldName));
            } else {
                ObjectNode baseObjectNode = (ObjectNode) base;
                if(fieldName.equals("pageJsonSeq") && !baseObjectNode.get(fieldName).isEmpty()) {
                    JsonNode tempValue = getPageSeqValue((ArrayNode) baseObjectNode.get(fieldName), (ArrayNode) value.get(fieldName));
                    baseObjectNode.set(fieldName, tempValue);
                }
                else
                    baseObjectNode.set(fieldName, value.get(fieldName));
            }
        }
    }

    private JsonNode getPageSeqValue(ArrayNode base, ArrayNode value) {
        if(value.isEmpty())
            return base;
        for(JsonNode arrayNode : value){
            if(!base.toString().contains(arrayNode.toString()))
                base.add(arrayNode);
        }
        return base;
    }

    public void mergeInBaseForMaxIdMap (JsonNode base, JsonNode value) {
        Iterator<String> fieldsIterator = value.fieldNames();

        while (fieldsIterator.hasNext()) {
            String fieldName = fieldsIterator.next();
            if (base.has(fieldName) && base.get(fieldName).isObject()) {
                mergeInBaseForMaxIdMap(base.get(fieldName), value.get(fieldName));
            } else {
                ObjectNode baseObjectNode = (ObjectNode) base;

                JsonNode tempValue= value.get(fieldName);

                if(baseObjectNode.get(fieldName)!=null && baseObjectNode.get(fieldName).longValue()>value.get(fieldName).longValue()){
                    tempValue = baseObjectNode.get(fieldName);
                }

                baseObjectNode.set(fieldName,tempValue );
            }
        }
    }

    private void addToArray(JsonPointer path, JsonNode value, JsonNode parentNode) {
        final ArrayNode target = (ArrayNode) parentNode;
        int idx = path.last().getIndex();

        if (idx == JsonPointer.LAST_INDEX) {
            // see http://tools.ietf.org/html/rfc6902#section-4.1
            target.add(value);
        } else {
            if (idx > target.size())
                throw new JsonPatchApplicationException(
                        "Array index " + idx + " out of bounds", Operation.ADD, path.getParent());
            target.insert(idx, value);
        }
    }
}

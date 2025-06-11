'use strict';

import {AttributeList} from '../../../main/codec/proto';
import {createAttributeListPayload} from '../../../main/codec/messages';

describe('attribute-related message codec', () => {

  test('attributeList with one entry', () => {
    const attributeList = createAttributeListPayload('hello');
    expect(AttributeList.verify(attributeList)).toBe(null);
    expect(attributeList.attributes[0]).toBe('hello');

    const message = AttributeList.create(attributeList);

    // run encode and decode
    const buffer = AttributeList.encode(message).finish();
    const decoded = AttributeList.decode(buffer);
    const rebuilt = AttributeList.toObject(decoded);

    expect(rebuilt.attributes.length).toBe(1);
    expect(rebuilt.attributes[0]).toBe('hello');
  });

  test('attributeList with zero entries', () => {
    const attributeList = createAttributeListPayload();
    expect(AttributeList.verify(attributeList)).toBe(null);
    expect(attributeList.attributes.length).toBe(0);

    const message = AttributeList.create(attributeList);

    // run encode and decode
    const buffer = AttributeList.encode(message).finish();
    const decoded = AttributeList.decode(buffer);
    const rebuilt = AttributeList.toObject(decoded);

    expect(rebuilt.attributes).toBe(undefined);
  });

  test('attributeList with three entries', () => {
    const attributeList = createAttributeListPayload('Hello', 'getname()', 'world');
    expect(AttributeList.verify(attributeList)).toBe(null);
    expect(attributeList.attributes.length).toBe(3);

    const message = AttributeList.create(attributeList);

    // run encode and decode
    const buffer = AttributeList.encode(message).finish();
    const decoded = AttributeList.decode(buffer);
    const rebuilt = AttributeList.toObject(decoded);

    expect(rebuilt.attributes.length).toBe(3);
    expect(rebuilt.attributes[0]).toBe('Hello');
    expect(rebuilt.attributes[1]).toBe('getname()');
    expect(rebuilt.attributes[2]).toBe('world');
  });
});

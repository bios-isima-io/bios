'use strict';

import {Dimensions} from '../../../main/codec/proto';
import {createDimensionsPayload} from '../../../main/codec/messages';

describe('dimensions message codec', () => {

  test('dimensions with one entry', () => {
    const dimensions = createDimensionsPayload('hello');
    expect(Dimensions.verify(dimensions)).toBe(null);
    expect(dimensions.dimensions[0]).toBe('hello');

    const message = Dimensions.create(dimensions);

    // run encode and decode
    const buffer = Dimensions.encode(message).finish();
    const decoded = Dimensions.decode(buffer);
    const rebuilt = Dimensions.toObject(decoded);

    expect(rebuilt.dimensions.length).toBe(1);
    expect(rebuilt.dimensions[0]).toBe('hello');
  });

  test('dimensions with zero entries', () => {
    const dimensions = createDimensionsPayload();
    expect(Dimensions.verify(dimensions)).toBe(null);
    expect(dimensions.dimensions.length).toBe(0);

    const message = Dimensions.create(dimensions);

    // run encode and decode
    const buffer = Dimensions.encode(message).finish();
    const decoded = Dimensions.decode(buffer);
    const rebuilt = Dimensions.toObject(decoded);

    expect(rebuilt.dimensions).toBe(undefined);
  });

  test('dimensions with three entries', () => {
    const dimensions = createDimensionsPayload('Country', 'state', 'city');
    expect(Dimensions.verify(dimensions)).toBe(null);
    expect(dimensions.dimensions.length).toBe(3);

    const message = Dimensions.create(dimensions);

    // run encode and decode
    const buffer = Dimensions.encode(message).finish();
    const decoded = Dimensions.decode(buffer);
    const rebuilt = Dimensions.toObject(decoded);

    expect(rebuilt.dimensions.length).toBe(3);
    expect(rebuilt.dimensions[0]).toBe('Country');
    expect(rebuilt.dimensions[1]).toBe('state');
    expect(rebuilt.dimensions[2]).toBe('city');
  });
});

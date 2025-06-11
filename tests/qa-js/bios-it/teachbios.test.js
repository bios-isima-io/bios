/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, {
  BiosClientError,
  BiosErrorType
} from 'bios-sdk';
import { BIOS_ENDPOINT } from './tutils';


describe('TeachBios', () => {
  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'admin@jsSimpleTest',
      password: 'admin',
    });
  });

  afterAll(async () => {
    await bios.signOut();
  });

  it('Single entry', async () => {
    const results = await bios.teachBios('hello,123,true');
    expect(results).toBeDefined();
    expect(results.attributes).toBeDefined();
    expect(results.attributes.length).toBe(3);
    expect(results.attributes[0].type).toBe('string');
    expect(results.attributes[1].type).toBe('int');
    expect(results.attributes[2].type).toBe('boolean');
  });

  it('Multiple entries', async () => {
    const entries = [
      'hello,world,123,45,true,false,true',
      'hi,there,456,991.2,false,123,yes',
      'greetings,0,712,13.4,true,true,false'
    ]
    const results = await bios.teachBios(entries.join('\n'));
    expect(results).toBeDefined();
    expect(results.attributes).toBeDefined();
    expect(results.attributes.length).toBe(7);
    expect(results.attributes[0].type).toBe('string');
    expect(results.attributes[1].type).toBe('string');
    expect(results.attributes[2].type).toBe('int');
    expect(results.attributes[3].type).toBe('double');
    expect(results.attributes[4].type).toBe('boolean');
    expect(results.attributes[5].type).toBe('string');
    expect(results.attributes[6].type).toBe('string');
  });
});

import {generateTimeUuid} from '../../../main/common/utils';
import {createUuidPayload, parseUuidMessage} from '../../../main/codec/messages';
import {Uuid} from '../../../main/codec/proto';

describe('UUID payload creation test', () => {
  test('fundamental', () => {
    const uuid = generateTimeUuid();
    const uuidPayload = createUuidPayload(uuid);

    const buffer = Uuid.encode(uuidPayload).finish();
    const decoded = Uuid.decode(buffer);

    const rebuilt = parseUuidMessage(decoded);
    expect(rebuilt).toEqual(uuid);
  });
})
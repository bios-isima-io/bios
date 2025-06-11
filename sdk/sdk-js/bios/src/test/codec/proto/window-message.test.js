'use strict';

import {Window, WindowType} from '../../../main/codec/proto';
import {
  createTumblingWindowPayload,
  createSlidingWindowPayload
} from '../../../main/codec/messages';

describe('window message codec', () => {

  test('tumbling window', () => {
    const windowSize = 300000;

    // test building a tumbling window message
    const window = createTumblingWindowPayload(windowSize);
    expect(Window.verify(window)).toBe(null);
    expect(window.windowType).toBe(WindowType.TUMBLING_WINDOW);
    expect(window.tumbling.windowSizeMs).toBe(windowSize);

    const message = Window.create(window);
    // console.log('MESSAGE:', message);

    // run encode and decode
    const buffer = Window.encode(message).finish();
    const decoded = Window.decode(buffer);
    const rebuilt = Window.toObject(decoded, {
      longs: Number,
    });

    // console.log('DECODED:', rebuilt);
    expect(rebuilt.windowType).toBe(WindowType.TUMBLING_WINDOW);
    expect(rebuilt.tumbling.windowSizeMs).toBe(windowSize);
  });

  test('sliding window', () => {
    const slideInterval = 600000;
    const windowSlides = 4;

    const window = createSlidingWindowPayload(slideInterval, windowSlides);
    expect(Window.verify(window)).toBe(null);
    expect(window.windowType).toBe(WindowType.SLIDING_WINDOW);
    expect(window.sliding.slideInterval).toBe(slideInterval);
    expect(window.sliding.windowSlides).toBe(windowSlides);

    const message = Window.create(window);

    // run encode and decode
    const buffer = Window.encode(message).finish();
    const decoded = Window.decode(buffer);
    const rebuilt = Window.toObject(decoded, {
      longs: Number,
    });

    expect(rebuilt.windowType).toBe(WindowType.SLIDING_WINDOW);
    expect(rebuilt.sliding.slideInterval).toBe(slideInterval);
    expect(rebuilt.sliding.windowSlides).toBe(windowSlides);
  });
});

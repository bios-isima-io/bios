/**
 * ISql operation request class
 */
export class ISqlRequest {
  /**
   * The constructor.
   *
   * @param {string} type - Statement type.
   */
  constructor(type) {
    this._type = type;
  }

  get type() {
    return this._type;
  }
}

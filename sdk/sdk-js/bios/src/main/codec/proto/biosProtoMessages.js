/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
(function(global, factory) { /* global define, require, module */

    /* AMD */ if (typeof define === 'function' && define.amd)
        define(["protobufjs/minimal"], factory);

    /* CommonJS */ else if (typeof require === 'function' && typeof module === 'object' && module && module.exports)
        module.exports = factory(require("protobufjs/minimal"));

})(this, function($protobuf) {
    "use strict";

    // Common aliases
    var $Reader = $protobuf.Reader, $Writer = $protobuf.Writer, $util = $protobuf.util;
    
    // Exported root namespace
    var $root = $protobuf.roots["default"] || ($protobuf.roots["default"] = {});
    
    $root.com = (function() {
    
        /**
         * Namespace com.
         * @exports com
         * @namespace
         */
        var com = {};
    
        com.isima = (function() {
    
            /**
             * Namespace isima.
             * @memberof com
             * @namespace
             */
            var isima = {};
    
            isima.bios = (function() {
    
                /**
                 * Namespace bios.
                 * @memberof com.isima
                 * @namespace
                 */
                var bios = {};
    
                bios.models = (function() {
    
                    /**
                     * Namespace models.
                     * @memberof com.isima.bios
                     * @namespace
                     */
                    var models = {};
    
                    models.proto = (function() {
    
                        /**
                         * Namespace proto.
                         * @memberof com.isima.bios.models
                         * @namespace
                         */
                        var proto = {};
    
                        proto.Uuid = (function() {
    
                            /**
                             * Properties of an Uuid.
                             * @memberof com.isima.bios.models.proto
                             * @interface IUuid
                             * @property {number|Long|null} [hi] Uuid hi
                             * @property {number|Long|null} [lo] Uuid lo
                             */
    
                            /**
                             * Constructs a new Uuid.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an Uuid.
                             * @implements IUuid
                             * @constructor
                             * @param {com.isima.bios.models.proto.IUuid=} [properties] Properties to set
                             */
                            function Uuid(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * Uuid hi.
                             * @member {number|Long} hi
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @instance
                             */
                            Uuid.prototype.hi = $util.Long ? $util.Long.fromBits(0,0,true) : 0;
    
                            /**
                             * Uuid lo.
                             * @member {number|Long} lo
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @instance
                             */
                            Uuid.prototype.lo = $util.Long ? $util.Long.fromBits(0,0,true) : 0;
    
                            /**
                             * Creates a new Uuid instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {com.isima.bios.models.proto.IUuid=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.Uuid} Uuid instance
                             */
                            Uuid.create = function create(properties) {
                                return new Uuid(properties);
                            };
    
                            /**
                             * Encodes the specified Uuid message. Does not implicitly {@link com.isima.bios.models.proto.Uuid.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {com.isima.bios.models.proto.IUuid} message Uuid message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Uuid.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.hi != null && Object.hasOwnProperty.call(message, "hi"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).uint64(message.hi);
                                if (message.lo != null && Object.hasOwnProperty.call(message, "lo"))
                                    writer.uint32(/* id 2, wireType 0 =*/16).uint64(message.lo);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified Uuid message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.Uuid.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {com.isima.bios.models.proto.IUuid} message Uuid message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Uuid.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an Uuid message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.Uuid} Uuid
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Uuid.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.Uuid();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.hi = reader.uint64();
                                        break;
                                    case 2:
                                        message.lo = reader.uint64();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an Uuid message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.Uuid} Uuid
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Uuid.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an Uuid message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            Uuid.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.hi != null && message.hasOwnProperty("hi"))
                                    if (!$util.isInteger(message.hi) && !(message.hi && $util.isInteger(message.hi.low) && $util.isInteger(message.hi.high)))
                                        return "hi: integer|Long expected";
                                if (message.lo != null && message.hasOwnProperty("lo"))
                                    if (!$util.isInteger(message.lo) && !(message.lo && $util.isInteger(message.lo.low) && $util.isInteger(message.lo.high)))
                                        return "lo: integer|Long expected";
                                return null;
                            };
    
                            /**
                             * Creates an Uuid message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.Uuid} Uuid
                             */
                            Uuid.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.Uuid)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.Uuid();
                                if (object.hi != null)
                                    if ($util.Long)
                                        (message.hi = $util.Long.fromValue(object.hi)).unsigned = true;
                                    else if (typeof object.hi === "string")
                                        message.hi = parseInt(object.hi, 10);
                                    else if (typeof object.hi === "number")
                                        message.hi = object.hi;
                                    else if (typeof object.hi === "object")
                                        message.hi = new $util.LongBits(object.hi.low >>> 0, object.hi.high >>> 0).toNumber(true);
                                if (object.lo != null)
                                    if ($util.Long)
                                        (message.lo = $util.Long.fromValue(object.lo)).unsigned = true;
                                    else if (typeof object.lo === "string")
                                        message.lo = parseInt(object.lo, 10);
                                    else if (typeof object.lo === "number")
                                        message.lo = object.lo;
                                    else if (typeof object.lo === "object")
                                        message.lo = new $util.LongBits(object.lo.low >>> 0, object.lo.high >>> 0).toNumber(true);
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an Uuid message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @static
                             * @param {com.isima.bios.models.proto.Uuid} message Uuid
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            Uuid.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, true);
                                        object.hi = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.hi = options.longs === String ? "0" : 0;
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, true);
                                        object.lo = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.lo = options.longs === String ? "0" : 0;
                                }
                                if (message.hi != null && message.hasOwnProperty("hi"))
                                    if (typeof message.hi === "number")
                                        object.hi = options.longs === String ? String(message.hi) : message.hi;
                                    else
                                        object.hi = options.longs === String ? $util.Long.prototype.toString.call(message.hi) : options.longs === Number ? new $util.LongBits(message.hi.low >>> 0, message.hi.high >>> 0).toNumber(true) : message.hi;
                                if (message.lo != null && message.hasOwnProperty("lo"))
                                    if (typeof message.lo === "number")
                                        object.lo = options.longs === String ? String(message.lo) : message.lo;
                                    else
                                        object.lo = options.longs === String ? $util.Long.prototype.toString.call(message.lo) : options.longs === Number ? new $util.LongBits(message.lo.low >>> 0, message.lo.high >>> 0).toNumber(true) : message.lo;
                                return object;
                            };
    
                            /**
                             * Converts this Uuid to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.Uuid
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            Uuid.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return Uuid;
                        })();
    
                        proto.Dimensions = (function() {
    
                            /**
                             * Properties of a Dimensions.
                             * @memberof com.isima.bios.models.proto
                             * @interface IDimensions
                             * @property {Array.<string>|null} [dimensions] Dimensions dimensions
                             */
    
                            /**
                             * Constructs a new Dimensions.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a Dimensions.
                             * @implements IDimensions
                             * @constructor
                             * @param {com.isima.bios.models.proto.IDimensions=} [properties] Properties to set
                             */
                            function Dimensions(properties) {
                                this.dimensions = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * Dimensions dimensions.
                             * @member {Array.<string>} dimensions
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @instance
                             */
                            Dimensions.prototype.dimensions = $util.emptyArray;
    
                            /**
                             * Creates a new Dimensions instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {com.isima.bios.models.proto.IDimensions=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.Dimensions} Dimensions instance
                             */
                            Dimensions.create = function create(properties) {
                                return new Dimensions(properties);
                            };
    
                            /**
                             * Encodes the specified Dimensions message. Does not implicitly {@link com.isima.bios.models.proto.Dimensions.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {com.isima.bios.models.proto.IDimensions} message Dimensions message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Dimensions.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.dimensions != null && message.dimensions.length)
                                    for (var i = 0; i < message.dimensions.length; ++i)
                                        writer.uint32(/* id 1, wireType 2 =*/10).string(message.dimensions[i]);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified Dimensions message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.Dimensions.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {com.isima.bios.models.proto.IDimensions} message Dimensions message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Dimensions.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a Dimensions message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.Dimensions} Dimensions
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Dimensions.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.Dimensions();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        if (!(message.dimensions && message.dimensions.length))
                                            message.dimensions = [];
                                        message.dimensions.push(reader.string());
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a Dimensions message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.Dimensions} Dimensions
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Dimensions.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a Dimensions message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            Dimensions.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.dimensions != null && message.hasOwnProperty("dimensions")) {
                                    if (!Array.isArray(message.dimensions))
                                        return "dimensions: array expected";
                                    for (var i = 0; i < message.dimensions.length; ++i)
                                        if (!$util.isString(message.dimensions[i]))
                                            return "dimensions: string[] expected";
                                }
                                return null;
                            };
    
                            /**
                             * Creates a Dimensions message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.Dimensions} Dimensions
                             */
                            Dimensions.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.Dimensions)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.Dimensions();
                                if (object.dimensions) {
                                    if (!Array.isArray(object.dimensions))
                                        throw TypeError(".com.isima.bios.models.proto.Dimensions.dimensions: array expected");
                                    message.dimensions = [];
                                    for (var i = 0; i < object.dimensions.length; ++i)
                                        message.dimensions[i] = String(object.dimensions[i]);
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a Dimensions message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @static
                             * @param {com.isima.bios.models.proto.Dimensions} message Dimensions
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            Dimensions.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.dimensions = [];
                                if (message.dimensions && message.dimensions.length) {
                                    object.dimensions = [];
                                    for (var j = 0; j < message.dimensions.length; ++j)
                                        object.dimensions[j] = message.dimensions[j];
                                }
                                return object;
                            };
    
                            /**
                             * Converts this Dimensions to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.Dimensions
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            Dimensions.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return Dimensions;
                        })();
    
                        proto.OrderBy = (function() {
    
                            /**
                             * Properties of an OrderBy.
                             * @memberof com.isima.bios.models.proto
                             * @interface IOrderBy
                             * @property {string|null} [by] OrderBy by
                             * @property {boolean|null} [reverse] OrderBy reverse
                             * @property {boolean|null} [caseSensitive] OrderBy caseSensitive
                             */
    
                            /**
                             * Constructs a new OrderBy.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an OrderBy.
                             * @implements IOrderBy
                             * @constructor
                             * @param {com.isima.bios.models.proto.IOrderBy=} [properties] Properties to set
                             */
                            function OrderBy(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * OrderBy by.
                             * @member {string} by
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @instance
                             */
                            OrderBy.prototype.by = "";
    
                            /**
                             * OrderBy reverse.
                             * @member {boolean} reverse
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @instance
                             */
                            OrderBy.prototype.reverse = false;
    
                            /**
                             * OrderBy caseSensitive.
                             * @member {boolean} caseSensitive
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @instance
                             */
                            OrderBy.prototype.caseSensitive = false;
    
                            /**
                             * Creates a new OrderBy instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {com.isima.bios.models.proto.IOrderBy=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.OrderBy} OrderBy instance
                             */
                            OrderBy.create = function create(properties) {
                                return new OrderBy(properties);
                            };
    
                            /**
                             * Encodes the specified OrderBy message. Does not implicitly {@link com.isima.bios.models.proto.OrderBy.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {com.isima.bios.models.proto.IOrderBy} message OrderBy message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            OrderBy.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.by != null && Object.hasOwnProperty.call(message, "by"))
                                    writer.uint32(/* id 1, wireType 2 =*/10).string(message.by);
                                if (message.reverse != null && Object.hasOwnProperty.call(message, "reverse"))
                                    writer.uint32(/* id 2, wireType 0 =*/16).bool(message.reverse);
                                if (message.caseSensitive != null && Object.hasOwnProperty.call(message, "caseSensitive"))
                                    writer.uint32(/* id 3, wireType 0 =*/24).bool(message.caseSensitive);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified OrderBy message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.OrderBy.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {com.isima.bios.models.proto.IOrderBy} message OrderBy message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            OrderBy.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an OrderBy message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.OrderBy} OrderBy
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            OrderBy.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.OrderBy();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.by = reader.string();
                                        break;
                                    case 2:
                                        message.reverse = reader.bool();
                                        break;
                                    case 3:
                                        message.caseSensitive = reader.bool();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an OrderBy message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.OrderBy} OrderBy
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            OrderBy.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an OrderBy message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            OrderBy.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.by != null && message.hasOwnProperty("by"))
                                    if (!$util.isString(message.by))
                                        return "by: string expected";
                                if (message.reverse != null && message.hasOwnProperty("reverse"))
                                    if (typeof message.reverse !== "boolean")
                                        return "reverse: boolean expected";
                                if (message.caseSensitive != null && message.hasOwnProperty("caseSensitive"))
                                    if (typeof message.caseSensitive !== "boolean")
                                        return "caseSensitive: boolean expected";
                                return null;
                            };
    
                            /**
                             * Creates an OrderBy message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.OrderBy} OrderBy
                             */
                            OrderBy.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.OrderBy)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.OrderBy();
                                if (object.by != null)
                                    message.by = String(object.by);
                                if (object.reverse != null)
                                    message.reverse = Boolean(object.reverse);
                                if (object.caseSensitive != null)
                                    message.caseSensitive = Boolean(object.caseSensitive);
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an OrderBy message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @static
                             * @param {com.isima.bios.models.proto.OrderBy} message OrderBy
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            OrderBy.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    object.by = "";
                                    object.reverse = false;
                                    object.caseSensitive = false;
                                }
                                if (message.by != null && message.hasOwnProperty("by"))
                                    object.by = message.by;
                                if (message.reverse != null && message.hasOwnProperty("reverse"))
                                    object.reverse = message.reverse;
                                if (message.caseSensitive != null && message.hasOwnProperty("caseSensitive"))
                                    object.caseSensitive = message.caseSensitive;
                                return object;
                            };
    
                            /**
                             * Converts this OrderBy to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.OrderBy
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            OrderBy.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return OrderBy;
                        })();
    
                        /**
                         * MetricFunction enum.
                         * @name com.isima.bios.models.proto.MetricFunction
                         * @enum {number}
                         * @property {number} SUM=0 SUM value
                         * @property {number} COUNT=1 COUNT value
                         * @property {number} MIN=2 MIN value
                         * @property {number} MAX=3 MAX value
                         * @property {number} LAST=4 LAST value
                         * @property {number} AVG=5 AVG value
                         * @property {number} VARIANCE=6 VARIANCE value
                         * @property {number} STDDEV=7 STDDEV value
                         * @property {number} SKEWNESS=8 SKEWNESS value
                         * @property {number} KURTOSIS=9 KURTOSIS value
                         * @property {number} SUM2=10 SUM2 value
                         * @property {number} SUM3=11 SUM3 value
                         * @property {number} SUM4=12 SUM4 value
                         * @property {number} MEDIAN=31 MEDIAN value
                         * @property {number} P0_01=32 P0_01 value
                         * @property {number} P0_1=33 P0_1 value
                         * @property {number} P1=34 P1 value
                         * @property {number} P10=35 P10 value
                         * @property {number} P25=36 P25 value
                         * @property {number} P50=37 P50 value
                         * @property {number} P75=38 P75 value
                         * @property {number} P90=39 P90 value
                         * @property {number} P99=40 P99 value
                         * @property {number} P99_9=41 P99_9 value
                         * @property {number} P99_99=42 P99_99 value
                         * @property {number} DISTINCTCOUNT=61 DISTINCTCOUNT value
                         * @property {number} DCLB1=62 DCLB1 value
                         * @property {number} DCUB1=63 DCUB1 value
                         * @property {number} DCLB2=64 DCLB2 value
                         * @property {number} DCUB2=65 DCUB2 value
                         * @property {number} DCLB3=66 DCLB3 value
                         * @property {number} DCUB3=67 DCUB3 value
                         * @property {number} NUMSAMPLES=81 NUMSAMPLES value
                         * @property {number} SAMPLINGFRACTION=82 SAMPLINGFRACTION value
                         * @property {number} SAMPLECOUNTS=83 SAMPLECOUNTS value
                         * @property {number} SYNOPSIS=101 SYNOPSIS value
                         */
                        proto.MetricFunction = (function() {
                            var valuesById = {}, values = Object.create(valuesById);
                            values[valuesById[0] = "SUM"] = 0;
                            values[valuesById[1] = "COUNT"] = 1;
                            values[valuesById[2] = "MIN"] = 2;
                            values[valuesById[3] = "MAX"] = 3;
                            values[valuesById[4] = "LAST"] = 4;
                            values[valuesById[5] = "AVG"] = 5;
                            values[valuesById[6] = "VARIANCE"] = 6;
                            values[valuesById[7] = "STDDEV"] = 7;
                            values[valuesById[8] = "SKEWNESS"] = 8;
                            values[valuesById[9] = "KURTOSIS"] = 9;
                            values[valuesById[10] = "SUM2"] = 10;
                            values[valuesById[11] = "SUM3"] = 11;
                            values[valuesById[12] = "SUM4"] = 12;
                            values[valuesById[31] = "MEDIAN"] = 31;
                            values[valuesById[32] = "P0_01"] = 32;
                            values[valuesById[33] = "P0_1"] = 33;
                            values[valuesById[34] = "P1"] = 34;
                            values[valuesById[35] = "P10"] = 35;
                            values[valuesById[36] = "P25"] = 36;
                            values[valuesById[37] = "P50"] = 37;
                            values[valuesById[38] = "P75"] = 38;
                            values[valuesById[39] = "P90"] = 39;
                            values[valuesById[40] = "P99"] = 40;
                            values[valuesById[41] = "P99_9"] = 41;
                            values[valuesById[42] = "P99_99"] = 42;
                            values[valuesById[61] = "DISTINCTCOUNT"] = 61;
                            values[valuesById[62] = "DCLB1"] = 62;
                            values[valuesById[63] = "DCUB1"] = 63;
                            values[valuesById[64] = "DCLB2"] = 64;
                            values[valuesById[65] = "DCUB2"] = 65;
                            values[valuesById[66] = "DCLB3"] = 66;
                            values[valuesById[67] = "DCUB3"] = 67;
                            values[valuesById[81] = "NUMSAMPLES"] = 81;
                            values[valuesById[82] = "SAMPLINGFRACTION"] = 82;
                            values[valuesById[83] = "SAMPLECOUNTS"] = 83;
                            values[valuesById[101] = "SYNOPSIS"] = 101;
                            return values;
                        })();
    
                        proto.Metric = (function() {
    
                            /**
                             * Properties of a Metric.
                             * @memberof com.isima.bios.models.proto
                             * @interface IMetric
                             * @property {com.isima.bios.models.proto.MetricFunction|null} ["function"] Metric function
                             * @property {string|null} [of] Metric of
                             * @property {string|null} [as] Metric as
                             * @property {string|null} [name] Metric name
                             */
    
                            /**
                             * Constructs a new Metric.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a Metric.
                             * @implements IMetric
                             * @constructor
                             * @param {com.isima.bios.models.proto.IMetric=} [properties] Properties to set
                             */
                            function Metric(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * Metric function.
                             * @member {com.isima.bios.models.proto.MetricFunction} function
                             * @memberof com.isima.bios.models.proto.Metric
                             * @instance
                             */
                            Metric.prototype["function"] = 0;
    
                            /**
                             * Metric of.
                             * @member {string} of
                             * @memberof com.isima.bios.models.proto.Metric
                             * @instance
                             */
                            Metric.prototype.of = "";
    
                            /**
                             * Metric as.
                             * @member {string} as
                             * @memberof com.isima.bios.models.proto.Metric
                             * @instance
                             */
                            Metric.prototype.as = "";
    
                            /**
                             * Metric name.
                             * @member {string} name
                             * @memberof com.isima.bios.models.proto.Metric
                             * @instance
                             */
                            Metric.prototype.name = "";
    
                            /**
                             * Creates a new Metric instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {com.isima.bios.models.proto.IMetric=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.Metric} Metric instance
                             */
                            Metric.create = function create(properties) {
                                return new Metric(properties);
                            };
    
                            /**
                             * Encodes the specified Metric message. Does not implicitly {@link com.isima.bios.models.proto.Metric.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {com.isima.bios.models.proto.IMetric} message Metric message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Metric.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message["function"] != null && Object.hasOwnProperty.call(message, "function"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int32(message["function"]);
                                if (message.of != null && Object.hasOwnProperty.call(message, "of"))
                                    writer.uint32(/* id 2, wireType 2 =*/18).string(message.of);
                                if (message.as != null && Object.hasOwnProperty.call(message, "as"))
                                    writer.uint32(/* id 3, wireType 2 =*/26).string(message.as);
                                if (message.name != null && Object.hasOwnProperty.call(message, "name"))
                                    writer.uint32(/* id 4, wireType 2 =*/34).string(message.name);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified Metric message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.Metric.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {com.isima.bios.models.proto.IMetric} message Metric message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Metric.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a Metric message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.Metric} Metric
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Metric.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.Metric();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message["function"] = reader.int32();
                                        break;
                                    case 2:
                                        message.of = reader.string();
                                        break;
                                    case 3:
                                        message.as = reader.string();
                                        break;
                                    case 4:
                                        message.name = reader.string();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a Metric message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.Metric} Metric
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Metric.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a Metric message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            Metric.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message["function"] != null && message.hasOwnProperty("function"))
                                    switch (message["function"]) {
                                    default:
                                        return "function: enum value expected";
                                    case 0:
                                    case 1:
                                    case 2:
                                    case 3:
                                    case 4:
                                    case 5:
                                    case 6:
                                    case 7:
                                    case 8:
                                    case 9:
                                    case 10:
                                    case 11:
                                    case 12:
                                    case 31:
                                    case 32:
                                    case 33:
                                    case 34:
                                    case 35:
                                    case 36:
                                    case 37:
                                    case 38:
                                    case 39:
                                    case 40:
                                    case 41:
                                    case 42:
                                    case 61:
                                    case 62:
                                    case 63:
                                    case 64:
                                    case 65:
                                    case 66:
                                    case 67:
                                    case 81:
                                    case 82:
                                    case 83:
                                    case 101:
                                        break;
                                    }
                                if (message.of != null && message.hasOwnProperty("of"))
                                    if (!$util.isString(message.of))
                                        return "of: string expected";
                                if (message.as != null && message.hasOwnProperty("as"))
                                    if (!$util.isString(message.as))
                                        return "as: string expected";
                                if (message.name != null && message.hasOwnProperty("name"))
                                    if (!$util.isString(message.name))
                                        return "name: string expected";
                                return null;
                            };
    
                            /**
                             * Creates a Metric message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.Metric} Metric
                             */
                            Metric.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.Metric)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.Metric();
                                switch (object["function"]) {
                                case "SUM":
                                case 0:
                                    message["function"] = 0;
                                    break;
                                case "COUNT":
                                case 1:
                                    message["function"] = 1;
                                    break;
                                case "MIN":
                                case 2:
                                    message["function"] = 2;
                                    break;
                                case "MAX":
                                case 3:
                                    message["function"] = 3;
                                    break;
                                case "LAST":
                                case 4:
                                    message["function"] = 4;
                                    break;
                                case "AVG":
                                case 5:
                                    message["function"] = 5;
                                    break;
                                case "VARIANCE":
                                case 6:
                                    message["function"] = 6;
                                    break;
                                case "STDDEV":
                                case 7:
                                    message["function"] = 7;
                                    break;
                                case "SKEWNESS":
                                case 8:
                                    message["function"] = 8;
                                    break;
                                case "KURTOSIS":
                                case 9:
                                    message["function"] = 9;
                                    break;
                                case "SUM2":
                                case 10:
                                    message["function"] = 10;
                                    break;
                                case "SUM3":
                                case 11:
                                    message["function"] = 11;
                                    break;
                                case "SUM4":
                                case 12:
                                    message["function"] = 12;
                                    break;
                                case "MEDIAN":
                                case 31:
                                    message["function"] = 31;
                                    break;
                                case "P0_01":
                                case 32:
                                    message["function"] = 32;
                                    break;
                                case "P0_1":
                                case 33:
                                    message["function"] = 33;
                                    break;
                                case "P1":
                                case 34:
                                    message["function"] = 34;
                                    break;
                                case "P10":
                                case 35:
                                    message["function"] = 35;
                                    break;
                                case "P25":
                                case 36:
                                    message["function"] = 36;
                                    break;
                                case "P50":
                                case 37:
                                    message["function"] = 37;
                                    break;
                                case "P75":
                                case 38:
                                    message["function"] = 38;
                                    break;
                                case "P90":
                                case 39:
                                    message["function"] = 39;
                                    break;
                                case "P99":
                                case 40:
                                    message["function"] = 40;
                                    break;
                                case "P99_9":
                                case 41:
                                    message["function"] = 41;
                                    break;
                                case "P99_99":
                                case 42:
                                    message["function"] = 42;
                                    break;
                                case "DISTINCTCOUNT":
                                case 61:
                                    message["function"] = 61;
                                    break;
                                case "DCLB1":
                                case 62:
                                    message["function"] = 62;
                                    break;
                                case "DCUB1":
                                case 63:
                                    message["function"] = 63;
                                    break;
                                case "DCLB2":
                                case 64:
                                    message["function"] = 64;
                                    break;
                                case "DCUB2":
                                case 65:
                                    message["function"] = 65;
                                    break;
                                case "DCLB3":
                                case 66:
                                    message["function"] = 66;
                                    break;
                                case "DCUB3":
                                case 67:
                                    message["function"] = 67;
                                    break;
                                case "NUMSAMPLES":
                                case 81:
                                    message["function"] = 81;
                                    break;
                                case "SAMPLINGFRACTION":
                                case 82:
                                    message["function"] = 82;
                                    break;
                                case "SAMPLECOUNTS":
                                case 83:
                                    message["function"] = 83;
                                    break;
                                case "SYNOPSIS":
                                case 101:
                                    message["function"] = 101;
                                    break;
                                }
                                if (object.of != null)
                                    message.of = String(object.of);
                                if (object.as != null)
                                    message.as = String(object.as);
                                if (object.name != null)
                                    message.name = String(object.name);
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a Metric message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.Metric
                             * @static
                             * @param {com.isima.bios.models.proto.Metric} message Metric
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            Metric.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    object["function"] = options.enums === String ? "SUM" : 0;
                                    object.of = "";
                                    object.as = "";
                                    object.name = "";
                                }
                                if (message["function"] != null && message.hasOwnProperty("function"))
                                    object["function"] = options.enums === String ? $root.com.isima.bios.models.proto.MetricFunction[message["function"]] : message["function"];
                                if (message.of != null && message.hasOwnProperty("of"))
                                    object.of = message.of;
                                if (message.as != null && message.hasOwnProperty("as"))
                                    object.as = message.as;
                                if (message.name != null && message.hasOwnProperty("name"))
                                    object.name = message.name;
                                return object;
                            };
    
                            /**
                             * Converts this Metric to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.Metric
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            Metric.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return Metric;
                        })();
    
                        /**
                         * ContentRepresentation enum.
                         * @name com.isima.bios.models.proto.ContentRepresentation
                         * @enum {number}
                         * @property {number} NATIVE=0 NATIVE value
                         * @property {number} CSV=1 CSV value
                         * @property {number} UNTYPED=2 UNTYPED value
                         */
                        proto.ContentRepresentation = (function() {
                            var valuesById = {}, values = Object.create(valuesById);
                            values[valuesById[0] = "NATIVE"] = 0;
                            values[valuesById[1] = "CSV"] = 1;
                            values[valuesById[2] = "UNTYPED"] = 2;
                            return values;
                        })();
    
                        /**
                         * AttributeType enum.
                         * @name com.isima.bios.models.proto.AttributeType
                         * @enum {number}
                         * @property {number} UNKNOWN=0 UNKNOWN value
                         * @property {number} INTEGER=1 INTEGER value
                         * @property {number} DECIMAL=2 DECIMAL value
                         * @property {number} STRING=3 STRING value
                         * @property {number} BLOB=4 BLOB value
                         * @property {number} BOOLEAN=5 BOOLEAN value
                         */
                        proto.AttributeType = (function() {
                            var valuesById = {}, values = Object.create(valuesById);
                            values[valuesById[0] = "UNKNOWN"] = 0;
                            values[valuesById[1] = "INTEGER"] = 1;
                            values[valuesById[2] = "DECIMAL"] = 2;
                            values[valuesById[3] = "STRING"] = 3;
                            values[valuesById[4] = "BLOB"] = 4;
                            values[valuesById[5] = "BOOLEAN"] = 5;
                            return values;
                        })();
    
                        proto.ColumnDefinition = (function() {
    
                            /**
                             * Properties of a ColumnDefinition.
                             * @memberof com.isima.bios.models.proto
                             * @interface IColumnDefinition
                             * @property {com.isima.bios.models.proto.AttributeType|null} [type] ColumnDefinition type
                             * @property {string|null} [name] ColumnDefinition name
                             * @property {number|null} [indexInValueArray] ColumnDefinition indexInValueArray
                             */
    
                            /**
                             * Constructs a new ColumnDefinition.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a ColumnDefinition.
                             * @implements IColumnDefinition
                             * @constructor
                             * @param {com.isima.bios.models.proto.IColumnDefinition=} [properties] Properties to set
                             */
                            function ColumnDefinition(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * ColumnDefinition type.
                             * @member {com.isima.bios.models.proto.AttributeType} type
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @instance
                             */
                            ColumnDefinition.prototype.type = 0;
    
                            /**
                             * ColumnDefinition name.
                             * @member {string} name
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @instance
                             */
                            ColumnDefinition.prototype.name = "";
    
                            /**
                             * ColumnDefinition indexInValueArray.
                             * @member {number} indexInValueArray
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @instance
                             */
                            ColumnDefinition.prototype.indexInValueArray = 0;
    
                            /**
                             * Creates a new ColumnDefinition instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {com.isima.bios.models.proto.IColumnDefinition=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.ColumnDefinition} ColumnDefinition instance
                             */
                            ColumnDefinition.create = function create(properties) {
                                return new ColumnDefinition(properties);
                            };
    
                            /**
                             * Encodes the specified ColumnDefinition message. Does not implicitly {@link com.isima.bios.models.proto.ColumnDefinition.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {com.isima.bios.models.proto.IColumnDefinition} message ColumnDefinition message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            ColumnDefinition.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.type != null && Object.hasOwnProperty.call(message, "type"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int32(message.type);
                                if (message.name != null && Object.hasOwnProperty.call(message, "name"))
                                    writer.uint32(/* id 2, wireType 2 =*/18).string(message.name);
                                if (message.indexInValueArray != null && Object.hasOwnProperty.call(message, "indexInValueArray"))
                                    writer.uint32(/* id 3, wireType 0 =*/24).int32(message.indexInValueArray);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified ColumnDefinition message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.ColumnDefinition.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {com.isima.bios.models.proto.IColumnDefinition} message ColumnDefinition message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            ColumnDefinition.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a ColumnDefinition message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.ColumnDefinition} ColumnDefinition
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            ColumnDefinition.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.ColumnDefinition();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.type = reader.int32();
                                        break;
                                    case 2:
                                        message.name = reader.string();
                                        break;
                                    case 3:
                                        message.indexInValueArray = reader.int32();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a ColumnDefinition message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.ColumnDefinition} ColumnDefinition
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            ColumnDefinition.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a ColumnDefinition message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            ColumnDefinition.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.type != null && message.hasOwnProperty("type"))
                                    switch (message.type) {
                                    default:
                                        return "type: enum value expected";
                                    case 0:
                                    case 1:
                                    case 2:
                                    case 3:
                                    case 4:
                                    case 5:
                                        break;
                                    }
                                if (message.name != null && message.hasOwnProperty("name"))
                                    if (!$util.isString(message.name))
                                        return "name: string expected";
                                if (message.indexInValueArray != null && message.hasOwnProperty("indexInValueArray"))
                                    if (!$util.isInteger(message.indexInValueArray))
                                        return "indexInValueArray: integer expected";
                                return null;
                            };
    
                            /**
                             * Creates a ColumnDefinition message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.ColumnDefinition} ColumnDefinition
                             */
                            ColumnDefinition.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.ColumnDefinition)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.ColumnDefinition();
                                switch (object.type) {
                                case "UNKNOWN":
                                case 0:
                                    message.type = 0;
                                    break;
                                case "INTEGER":
                                case 1:
                                    message.type = 1;
                                    break;
                                case "DECIMAL":
                                case 2:
                                    message.type = 2;
                                    break;
                                case "STRING":
                                case 3:
                                    message.type = 3;
                                    break;
                                case "BLOB":
                                case 4:
                                    message.type = 4;
                                    break;
                                case "BOOLEAN":
                                case 5:
                                    message.type = 5;
                                    break;
                                }
                                if (object.name != null)
                                    message.name = String(object.name);
                                if (object.indexInValueArray != null)
                                    message.indexInValueArray = object.indexInValueArray | 0;
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a ColumnDefinition message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @static
                             * @param {com.isima.bios.models.proto.ColumnDefinition} message ColumnDefinition
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            ColumnDefinition.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    object.type = options.enums === String ? "UNKNOWN" : 0;
                                    object.name = "";
                                    object.indexInValueArray = 0;
                                }
                                if (message.type != null && message.hasOwnProperty("type"))
                                    object.type = options.enums === String ? $root.com.isima.bios.models.proto.AttributeType[message.type] : message.type;
                                if (message.name != null && message.hasOwnProperty("name"))
                                    object.name = message.name;
                                if (message.indexInValueArray != null && message.hasOwnProperty("indexInValueArray"))
                                    object.indexInValueArray = message.indexInValueArray;
                                return object;
                            };
    
                            /**
                             * Converts this ColumnDefinition to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.ColumnDefinition
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            ColumnDefinition.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return ColumnDefinition;
                        })();
    
                        proto.AttributeList = (function() {
    
                            /**
                             * Properties of an AttributeList.
                             * @memberof com.isima.bios.models.proto
                             * @interface IAttributeList
                             * @property {Array.<string>|null} [attributes] AttributeList attributes
                             */
    
                            /**
                             * Constructs a new AttributeList.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an AttributeList.
                             * @implements IAttributeList
                             * @constructor
                             * @param {com.isima.bios.models.proto.IAttributeList=} [properties] Properties to set
                             */
                            function AttributeList(properties) {
                                this.attributes = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * AttributeList attributes.
                             * @member {Array.<string>} attributes
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @instance
                             */
                            AttributeList.prototype.attributes = $util.emptyArray;
    
                            /**
                             * Creates a new AttributeList instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {com.isima.bios.models.proto.IAttributeList=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.AttributeList} AttributeList instance
                             */
                            AttributeList.create = function create(properties) {
                                return new AttributeList(properties);
                            };
    
                            /**
                             * Encodes the specified AttributeList message. Does not implicitly {@link com.isima.bios.models.proto.AttributeList.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {com.isima.bios.models.proto.IAttributeList} message AttributeList message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            AttributeList.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.attributes != null && message.attributes.length)
                                    for (var i = 0; i < message.attributes.length; ++i)
                                        writer.uint32(/* id 1, wireType 2 =*/10).string(message.attributes[i]);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified AttributeList message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.AttributeList.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {com.isima.bios.models.proto.IAttributeList} message AttributeList message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            AttributeList.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an AttributeList message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.AttributeList} AttributeList
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            AttributeList.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.AttributeList();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        if (!(message.attributes && message.attributes.length))
                                            message.attributes = [];
                                        message.attributes.push(reader.string());
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an AttributeList message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.AttributeList} AttributeList
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            AttributeList.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an AttributeList message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            AttributeList.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.attributes != null && message.hasOwnProperty("attributes")) {
                                    if (!Array.isArray(message.attributes))
                                        return "attributes: array expected";
                                    for (var i = 0; i < message.attributes.length; ++i)
                                        if (!$util.isString(message.attributes[i]))
                                            return "attributes: string[] expected";
                                }
                                return null;
                            };
    
                            /**
                             * Creates an AttributeList message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.AttributeList} AttributeList
                             */
                            AttributeList.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.AttributeList)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.AttributeList();
                                if (object.attributes) {
                                    if (!Array.isArray(object.attributes))
                                        throw TypeError(".com.isima.bios.models.proto.AttributeList.attributes: array expected");
                                    message.attributes = [];
                                    for (var i = 0; i < object.attributes.length; ++i)
                                        message.attributes[i] = String(object.attributes[i]);
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an AttributeList message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @static
                             * @param {com.isima.bios.models.proto.AttributeList} message AttributeList
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            AttributeList.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.attributes = [];
                                if (message.attributes && message.attributes.length) {
                                    object.attributes = [];
                                    for (var j = 0; j < message.attributes.length; ++j)
                                        object.attributes[j] = message.attributes[j];
                                }
                                return object;
                            };
    
                            /**
                             * Converts this AttributeList to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.AttributeList
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            AttributeList.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return AttributeList;
                        })();
    
                        proto.Record = (function() {
    
                            /**
                             * Properties of a Record.
                             * @memberof com.isima.bios.models.proto
                             * @interface IRecord
                             * @property {Uint8Array|null} [eventId] Record eventId
                             * @property {number|Long|null} [timestamp] Record timestamp
                             * @property {Array.<number|Long>|null} [longValues] Record longValues
                             * @property {Array.<number>|null} [doubleValues] Record doubleValues
                             * @property {Array.<string>|null} [stringValues] Record stringValues
                             * @property {Array.<Uint8Array>|null} [blobValues] Record blobValues
                             * @property {Array.<boolean>|null} [booleanValues] Record booleanValues
                             */
    
                            /**
                             * Constructs a new Record.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a Record.
                             * @implements IRecord
                             * @constructor
                             * @param {com.isima.bios.models.proto.IRecord=} [properties] Properties to set
                             */
                            function Record(properties) {
                                this.longValues = [];
                                this.doubleValues = [];
                                this.stringValues = [];
                                this.blobValues = [];
                                this.booleanValues = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * Record eventId.
                             * @member {Uint8Array} eventId
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             */
                            Record.prototype.eventId = $util.newBuffer([]);
    
                            /**
                             * Record timestamp.
                             * @member {number|Long} timestamp
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             */
                            Record.prototype.timestamp = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * Record longValues.
                             * @member {Array.<number|Long>} longValues
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             */
                            Record.prototype.longValues = $util.emptyArray;
    
                            /**
                             * Record doubleValues.
                             * @member {Array.<number>} doubleValues
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             */
                            Record.prototype.doubleValues = $util.emptyArray;
    
                            /**
                             * Record stringValues.
                             * @member {Array.<string>} stringValues
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             */
                            Record.prototype.stringValues = $util.emptyArray;
    
                            /**
                             * Record blobValues.
                             * @member {Array.<Uint8Array>} blobValues
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             */
                            Record.prototype.blobValues = $util.emptyArray;
    
                            /**
                             * Record booleanValues.
                             * @member {Array.<boolean>} booleanValues
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             */
                            Record.prototype.booleanValues = $util.emptyArray;
    
                            /**
                             * Creates a new Record instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {com.isima.bios.models.proto.IRecord=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.Record} Record instance
                             */
                            Record.create = function create(properties) {
                                return new Record(properties);
                            };
    
                            /**
                             * Encodes the specified Record message. Does not implicitly {@link com.isima.bios.models.proto.Record.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {com.isima.bios.models.proto.IRecord} message Record message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Record.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.eventId != null && Object.hasOwnProperty.call(message, "eventId"))
                                    writer.uint32(/* id 1, wireType 2 =*/10).bytes(message.eventId);
                                if (message.timestamp != null && Object.hasOwnProperty.call(message, "timestamp"))
                                    writer.uint32(/* id 2, wireType 0 =*/16).int64(message.timestamp);
                                if (message.longValues != null && message.longValues.length) {
                                    writer.uint32(/* id 10, wireType 2 =*/82).fork();
                                    for (var i = 0; i < message.longValues.length; ++i)
                                        writer.int64(message.longValues[i]);
                                    writer.ldelim();
                                }
                                if (message.doubleValues != null && message.doubleValues.length) {
                                    writer.uint32(/* id 11, wireType 2 =*/90).fork();
                                    for (var i = 0; i < message.doubleValues.length; ++i)
                                        writer.double(message.doubleValues[i]);
                                    writer.ldelim();
                                }
                                if (message.stringValues != null && message.stringValues.length)
                                    for (var i = 0; i < message.stringValues.length; ++i)
                                        writer.uint32(/* id 12, wireType 2 =*/98).string(message.stringValues[i]);
                                if (message.blobValues != null && message.blobValues.length)
                                    for (var i = 0; i < message.blobValues.length; ++i)
                                        writer.uint32(/* id 13, wireType 2 =*/106).bytes(message.blobValues[i]);
                                if (message.booleanValues != null && message.booleanValues.length) {
                                    writer.uint32(/* id 14, wireType 2 =*/114).fork();
                                    for (var i = 0; i < message.booleanValues.length; ++i)
                                        writer.bool(message.booleanValues[i]);
                                    writer.ldelim();
                                }
                                return writer;
                            };
    
                            /**
                             * Encodes the specified Record message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.Record.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {com.isima.bios.models.proto.IRecord} message Record message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Record.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a Record message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.Record} Record
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Record.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.Record();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.eventId = reader.bytes();
                                        break;
                                    case 2:
                                        message.timestamp = reader.int64();
                                        break;
                                    case 10:
                                        if (!(message.longValues && message.longValues.length))
                                            message.longValues = [];
                                        if ((tag & 7) === 2) {
                                            var end2 = reader.uint32() + reader.pos;
                                            while (reader.pos < end2)
                                                message.longValues.push(reader.int64());
                                        } else
                                            message.longValues.push(reader.int64());
                                        break;
                                    case 11:
                                        if (!(message.doubleValues && message.doubleValues.length))
                                            message.doubleValues = [];
                                        if ((tag & 7) === 2) {
                                            var end2 = reader.uint32() + reader.pos;
                                            while (reader.pos < end2)
                                                message.doubleValues.push(reader.double());
                                        } else
                                            message.doubleValues.push(reader.double());
                                        break;
                                    case 12:
                                        if (!(message.stringValues && message.stringValues.length))
                                            message.stringValues = [];
                                        message.stringValues.push(reader.string());
                                        break;
                                    case 13:
                                        if (!(message.blobValues && message.blobValues.length))
                                            message.blobValues = [];
                                        message.blobValues.push(reader.bytes());
                                        break;
                                    case 14:
                                        if (!(message.booleanValues && message.booleanValues.length))
                                            message.booleanValues = [];
                                        if ((tag & 7) === 2) {
                                            var end2 = reader.uint32() + reader.pos;
                                            while (reader.pos < end2)
                                                message.booleanValues.push(reader.bool());
                                        } else
                                            message.booleanValues.push(reader.bool());
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a Record message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.Record} Record
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Record.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a Record message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            Record.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.eventId != null && message.hasOwnProperty("eventId"))
                                    if (!(message.eventId && typeof message.eventId.length === "number" || $util.isString(message.eventId)))
                                        return "eventId: buffer expected";
                                if (message.timestamp != null && message.hasOwnProperty("timestamp"))
                                    if (!$util.isInteger(message.timestamp) && !(message.timestamp && $util.isInteger(message.timestamp.low) && $util.isInteger(message.timestamp.high)))
                                        return "timestamp: integer|Long expected";
                                if (message.longValues != null && message.hasOwnProperty("longValues")) {
                                    if (!Array.isArray(message.longValues))
                                        return "longValues: array expected";
                                    for (var i = 0; i < message.longValues.length; ++i)
                                        if (!$util.isInteger(message.longValues[i]) && !(message.longValues[i] && $util.isInteger(message.longValues[i].low) && $util.isInteger(message.longValues[i].high)))
                                            return "longValues: integer|Long[] expected";
                                }
                                if (message.doubleValues != null && message.hasOwnProperty("doubleValues")) {
                                    if (!Array.isArray(message.doubleValues))
                                        return "doubleValues: array expected";
                                    for (var i = 0; i < message.doubleValues.length; ++i)
                                        if (typeof message.doubleValues[i] !== "number")
                                            return "doubleValues: number[] expected";
                                }
                                if (message.stringValues != null && message.hasOwnProperty("stringValues")) {
                                    if (!Array.isArray(message.stringValues))
                                        return "stringValues: array expected";
                                    for (var i = 0; i < message.stringValues.length; ++i)
                                        if (!$util.isString(message.stringValues[i]))
                                            return "stringValues: string[] expected";
                                }
                                if (message.blobValues != null && message.hasOwnProperty("blobValues")) {
                                    if (!Array.isArray(message.blobValues))
                                        return "blobValues: array expected";
                                    for (var i = 0; i < message.blobValues.length; ++i)
                                        if (!(message.blobValues[i] && typeof message.blobValues[i].length === "number" || $util.isString(message.blobValues[i])))
                                            return "blobValues: buffer[] expected";
                                }
                                if (message.booleanValues != null && message.hasOwnProperty("booleanValues")) {
                                    if (!Array.isArray(message.booleanValues))
                                        return "booleanValues: array expected";
                                    for (var i = 0; i < message.booleanValues.length; ++i)
                                        if (typeof message.booleanValues[i] !== "boolean")
                                            return "booleanValues: boolean[] expected";
                                }
                                return null;
                            };
    
                            /**
                             * Creates a Record message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.Record} Record
                             */
                            Record.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.Record)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.Record();
                                if (object.eventId != null)
                                    if (typeof object.eventId === "string")
                                        $util.base64.decode(object.eventId, message.eventId = $util.newBuffer($util.base64.length(object.eventId)), 0);
                                    else if (object.eventId.length)
                                        message.eventId = object.eventId;
                                if (object.timestamp != null)
                                    if ($util.Long)
                                        (message.timestamp = $util.Long.fromValue(object.timestamp)).unsigned = false;
                                    else if (typeof object.timestamp === "string")
                                        message.timestamp = parseInt(object.timestamp, 10);
                                    else if (typeof object.timestamp === "number")
                                        message.timestamp = object.timestamp;
                                    else if (typeof object.timestamp === "object")
                                        message.timestamp = new $util.LongBits(object.timestamp.low >>> 0, object.timestamp.high >>> 0).toNumber();
                                if (object.longValues) {
                                    if (!Array.isArray(object.longValues))
                                        throw TypeError(".com.isima.bios.models.proto.Record.longValues: array expected");
                                    message.longValues = [];
                                    for (var i = 0; i < object.longValues.length; ++i)
                                        if ($util.Long)
                                            (message.longValues[i] = $util.Long.fromValue(object.longValues[i])).unsigned = false;
                                        else if (typeof object.longValues[i] === "string")
                                            message.longValues[i] = parseInt(object.longValues[i], 10);
                                        else if (typeof object.longValues[i] === "number")
                                            message.longValues[i] = object.longValues[i];
                                        else if (typeof object.longValues[i] === "object")
                                            message.longValues[i] = new $util.LongBits(object.longValues[i].low >>> 0, object.longValues[i].high >>> 0).toNumber();
                                }
                                if (object.doubleValues) {
                                    if (!Array.isArray(object.doubleValues))
                                        throw TypeError(".com.isima.bios.models.proto.Record.doubleValues: array expected");
                                    message.doubleValues = [];
                                    for (var i = 0; i < object.doubleValues.length; ++i)
                                        message.doubleValues[i] = Number(object.doubleValues[i]);
                                }
                                if (object.stringValues) {
                                    if (!Array.isArray(object.stringValues))
                                        throw TypeError(".com.isima.bios.models.proto.Record.stringValues: array expected");
                                    message.stringValues = [];
                                    for (var i = 0; i < object.stringValues.length; ++i)
                                        message.stringValues[i] = String(object.stringValues[i]);
                                }
                                if (object.blobValues) {
                                    if (!Array.isArray(object.blobValues))
                                        throw TypeError(".com.isima.bios.models.proto.Record.blobValues: array expected");
                                    message.blobValues = [];
                                    for (var i = 0; i < object.blobValues.length; ++i)
                                        if (typeof object.blobValues[i] === "string")
                                            $util.base64.decode(object.blobValues[i], message.blobValues[i] = $util.newBuffer($util.base64.length(object.blobValues[i])), 0);
                                        else if (object.blobValues[i].length)
                                            message.blobValues[i] = object.blobValues[i];
                                }
                                if (object.booleanValues) {
                                    if (!Array.isArray(object.booleanValues))
                                        throw TypeError(".com.isima.bios.models.proto.Record.booleanValues: array expected");
                                    message.booleanValues = [];
                                    for (var i = 0; i < object.booleanValues.length; ++i)
                                        message.booleanValues[i] = Boolean(object.booleanValues[i]);
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a Record message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.Record
                             * @static
                             * @param {com.isima.bios.models.proto.Record} message Record
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            Record.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults) {
                                    object.longValues = [];
                                    object.doubleValues = [];
                                    object.stringValues = [];
                                    object.blobValues = [];
                                    object.booleanValues = [];
                                }
                                if (options.defaults) {
                                    if (options.bytes === String)
                                        object.eventId = "";
                                    else {
                                        object.eventId = [];
                                        if (options.bytes !== Array)
                                            object.eventId = $util.newBuffer(object.eventId);
                                    }
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.timestamp = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.timestamp = options.longs === String ? "0" : 0;
                                }
                                if (message.eventId != null && message.hasOwnProperty("eventId"))
                                    object.eventId = options.bytes === String ? $util.base64.encode(message.eventId, 0, message.eventId.length) : options.bytes === Array ? Array.prototype.slice.call(message.eventId) : message.eventId;
                                if (message.timestamp != null && message.hasOwnProperty("timestamp"))
                                    if (typeof message.timestamp === "number")
                                        object.timestamp = options.longs === String ? String(message.timestamp) : message.timestamp;
                                    else
                                        object.timestamp = options.longs === String ? $util.Long.prototype.toString.call(message.timestamp) : options.longs === Number ? new $util.LongBits(message.timestamp.low >>> 0, message.timestamp.high >>> 0).toNumber() : message.timestamp;
                                if (message.longValues && message.longValues.length) {
                                    object.longValues = [];
                                    for (var j = 0; j < message.longValues.length; ++j)
                                        if (typeof message.longValues[j] === "number")
                                            object.longValues[j] = options.longs === String ? String(message.longValues[j]) : message.longValues[j];
                                        else
                                            object.longValues[j] = options.longs === String ? $util.Long.prototype.toString.call(message.longValues[j]) : options.longs === Number ? new $util.LongBits(message.longValues[j].low >>> 0, message.longValues[j].high >>> 0).toNumber() : message.longValues[j];
                                }
                                if (message.doubleValues && message.doubleValues.length) {
                                    object.doubleValues = [];
                                    for (var j = 0; j < message.doubleValues.length; ++j)
                                        object.doubleValues[j] = options.json && !isFinite(message.doubleValues[j]) ? String(message.doubleValues[j]) : message.doubleValues[j];
                                }
                                if (message.stringValues && message.stringValues.length) {
                                    object.stringValues = [];
                                    for (var j = 0; j < message.stringValues.length; ++j)
                                        object.stringValues[j] = message.stringValues[j];
                                }
                                if (message.blobValues && message.blobValues.length) {
                                    object.blobValues = [];
                                    for (var j = 0; j < message.blobValues.length; ++j)
                                        object.blobValues[j] = options.bytes === String ? $util.base64.encode(message.blobValues[j], 0, message.blobValues[j].length) : options.bytes === Array ? Array.prototype.slice.call(message.blobValues[j]) : message.blobValues[j];
                                }
                                if (message.booleanValues && message.booleanValues.length) {
                                    object.booleanValues = [];
                                    for (var j = 0; j < message.booleanValues.length; ++j)
                                        object.booleanValues[j] = message.booleanValues[j];
                                }
                                return object;
                            };
    
                            /**
                             * Converts this Record to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.Record
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            Record.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return Record;
                        })();
    
                        proto.InsertRequest = (function() {
    
                            /**
                             * Properties of an InsertRequest.
                             * @memberof com.isima.bios.models.proto
                             * @interface IInsertRequest
                             * @property {com.isima.bios.models.proto.ContentRepresentation|null} [contentRep] InsertRequest contentRep
                             * @property {com.isima.bios.models.proto.IRecord|null} [record] InsertRequest record
                             * @property {number|Long|null} [schemaVersion] InsertRequest schemaVersion
                             */
    
                            /**
                             * Constructs a new InsertRequest.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an InsertRequest.
                             * @implements IInsertRequest
                             * @constructor
                             * @param {com.isima.bios.models.proto.IInsertRequest=} [properties] Properties to set
                             */
                            function InsertRequest(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * InsertRequest contentRep.
                             * @member {com.isima.bios.models.proto.ContentRepresentation} contentRep
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @instance
                             */
                            InsertRequest.prototype.contentRep = 0;
    
                            /**
                             * InsertRequest record.
                             * @member {com.isima.bios.models.proto.IRecord|null|undefined} record
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @instance
                             */
                            InsertRequest.prototype.record = null;
    
                            /**
                             * InsertRequest schemaVersion.
                             * @member {number|Long} schemaVersion
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @instance
                             */
                            InsertRequest.prototype.schemaVersion = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * Creates a new InsertRequest instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertRequest=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.InsertRequest} InsertRequest instance
                             */
                            InsertRequest.create = function create(properties) {
                                return new InsertRequest(properties);
                            };
    
                            /**
                             * Encodes the specified InsertRequest message. Does not implicitly {@link com.isima.bios.models.proto.InsertRequest.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertRequest} message InsertRequest message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertRequest.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.contentRep != null && Object.hasOwnProperty.call(message, "contentRep"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int32(message.contentRep);
                                if (message.record != null && Object.hasOwnProperty.call(message, "record"))
                                    $root.com.isima.bios.models.proto.Record.encode(message.record, writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
                                if (message.schemaVersion != null && Object.hasOwnProperty.call(message, "schemaVersion"))
                                    writer.uint32(/* id 10, wireType 0 =*/80).int64(message.schemaVersion);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified InsertRequest message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.InsertRequest.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertRequest} message InsertRequest message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertRequest.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an InsertRequest message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.InsertRequest} InsertRequest
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertRequest.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.InsertRequest();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.contentRep = reader.int32();
                                        break;
                                    case 2:
                                        message.record = $root.com.isima.bios.models.proto.Record.decode(reader, reader.uint32());
                                        break;
                                    case 10:
                                        message.schemaVersion = reader.int64();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an InsertRequest message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.InsertRequest} InsertRequest
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertRequest.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an InsertRequest message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            InsertRequest.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.contentRep != null && message.hasOwnProperty("contentRep"))
                                    switch (message.contentRep) {
                                    default:
                                        return "contentRep: enum value expected";
                                    case 0:
                                    case 1:
                                    case 2:
                                        break;
                                    }
                                if (message.record != null && message.hasOwnProperty("record")) {
                                    var error = $root.com.isima.bios.models.proto.Record.verify(message.record);
                                    if (error)
                                        return "record." + error;
                                }
                                if (message.schemaVersion != null && message.hasOwnProperty("schemaVersion"))
                                    if (!$util.isInteger(message.schemaVersion) && !(message.schemaVersion && $util.isInteger(message.schemaVersion.low) && $util.isInteger(message.schemaVersion.high)))
                                        return "schemaVersion: integer|Long expected";
                                return null;
                            };
    
                            /**
                             * Creates an InsertRequest message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.InsertRequest} InsertRequest
                             */
                            InsertRequest.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.InsertRequest)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.InsertRequest();
                                switch (object.contentRep) {
                                case "NATIVE":
                                case 0:
                                    message.contentRep = 0;
                                    break;
                                case "CSV":
                                case 1:
                                    message.contentRep = 1;
                                    break;
                                case "UNTYPED":
                                case 2:
                                    message.contentRep = 2;
                                    break;
                                }
                                if (object.record != null) {
                                    if (typeof object.record !== "object")
                                        throw TypeError(".com.isima.bios.models.proto.InsertRequest.record: object expected");
                                    message.record = $root.com.isima.bios.models.proto.Record.fromObject(object.record);
                                }
                                if (object.schemaVersion != null)
                                    if ($util.Long)
                                        (message.schemaVersion = $util.Long.fromValue(object.schemaVersion)).unsigned = false;
                                    else if (typeof object.schemaVersion === "string")
                                        message.schemaVersion = parseInt(object.schemaVersion, 10);
                                    else if (typeof object.schemaVersion === "number")
                                        message.schemaVersion = object.schemaVersion;
                                    else if (typeof object.schemaVersion === "object")
                                        message.schemaVersion = new $util.LongBits(object.schemaVersion.low >>> 0, object.schemaVersion.high >>> 0).toNumber();
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an InsertRequest message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @static
                             * @param {com.isima.bios.models.proto.InsertRequest} message InsertRequest
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            InsertRequest.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    object.contentRep = options.enums === String ? "NATIVE" : 0;
                                    object.record = null;
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.schemaVersion = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.schemaVersion = options.longs === String ? "0" : 0;
                                }
                                if (message.contentRep != null && message.hasOwnProperty("contentRep"))
                                    object.contentRep = options.enums === String ? $root.com.isima.bios.models.proto.ContentRepresentation[message.contentRep] : message.contentRep;
                                if (message.record != null && message.hasOwnProperty("record"))
                                    object.record = $root.com.isima.bios.models.proto.Record.toObject(message.record, options);
                                if (message.schemaVersion != null && message.hasOwnProperty("schemaVersion"))
                                    if (typeof message.schemaVersion === "number")
                                        object.schemaVersion = options.longs === String ? String(message.schemaVersion) : message.schemaVersion;
                                    else
                                        object.schemaVersion = options.longs === String ? $util.Long.prototype.toString.call(message.schemaVersion) : options.longs === Number ? new $util.LongBits(message.schemaVersion.low >>> 0, message.schemaVersion.high >>> 0).toNumber() : message.schemaVersion;
                                return object;
                            };
    
                            /**
                             * Converts this InsertRequest to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.InsertRequest
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            InsertRequest.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return InsertRequest;
                        })();
    
                        proto.InsertSuccessResponse = (function() {
    
                            /**
                             * Properties of an InsertSuccessResponse.
                             * @memberof com.isima.bios.models.proto
                             * @interface IInsertSuccessResponse
                             * @property {Uint8Array|null} [eventId] InsertSuccessResponse eventId
                             * @property {number|Long|null} [insertTimestamp] InsertSuccessResponse insertTimestamp
                             */
    
                            /**
                             * Constructs a new InsertSuccessResponse.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an InsertSuccessResponse.
                             * @implements IInsertSuccessResponse
                             * @constructor
                             * @param {com.isima.bios.models.proto.IInsertSuccessResponse=} [properties] Properties to set
                             */
                            function InsertSuccessResponse(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * InsertSuccessResponse eventId.
                             * @member {Uint8Array} eventId
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @instance
                             */
                            InsertSuccessResponse.prototype.eventId = $util.newBuffer([]);
    
                            /**
                             * InsertSuccessResponse insertTimestamp.
                             * @member {number|Long} insertTimestamp
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @instance
                             */
                            InsertSuccessResponse.prototype.insertTimestamp = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * Creates a new InsertSuccessResponse instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertSuccessResponse=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.InsertSuccessResponse} InsertSuccessResponse instance
                             */
                            InsertSuccessResponse.create = function create(properties) {
                                return new InsertSuccessResponse(properties);
                            };
    
                            /**
                             * Encodes the specified InsertSuccessResponse message. Does not implicitly {@link com.isima.bios.models.proto.InsertSuccessResponse.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertSuccessResponse} message InsertSuccessResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertSuccessResponse.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.eventId != null && Object.hasOwnProperty.call(message, "eventId"))
                                    writer.uint32(/* id 1, wireType 2 =*/10).bytes(message.eventId);
                                if (message.insertTimestamp != null && Object.hasOwnProperty.call(message, "insertTimestamp"))
                                    writer.uint32(/* id 2, wireType 0 =*/16).int64(message.insertTimestamp);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified InsertSuccessResponse message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.InsertSuccessResponse.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertSuccessResponse} message InsertSuccessResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertSuccessResponse.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an InsertSuccessResponse message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.InsertSuccessResponse} InsertSuccessResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertSuccessResponse.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.InsertSuccessResponse();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.eventId = reader.bytes();
                                        break;
                                    case 2:
                                        message.insertTimestamp = reader.int64();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an InsertSuccessResponse message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.InsertSuccessResponse} InsertSuccessResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertSuccessResponse.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an InsertSuccessResponse message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            InsertSuccessResponse.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.eventId != null && message.hasOwnProperty("eventId"))
                                    if (!(message.eventId && typeof message.eventId.length === "number" || $util.isString(message.eventId)))
                                        return "eventId: buffer expected";
                                if (message.insertTimestamp != null && message.hasOwnProperty("insertTimestamp"))
                                    if (!$util.isInteger(message.insertTimestamp) && !(message.insertTimestamp && $util.isInteger(message.insertTimestamp.low) && $util.isInteger(message.insertTimestamp.high)))
                                        return "insertTimestamp: integer|Long expected";
                                return null;
                            };
    
                            /**
                             * Creates an InsertSuccessResponse message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.InsertSuccessResponse} InsertSuccessResponse
                             */
                            InsertSuccessResponse.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.InsertSuccessResponse)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.InsertSuccessResponse();
                                if (object.eventId != null)
                                    if (typeof object.eventId === "string")
                                        $util.base64.decode(object.eventId, message.eventId = $util.newBuffer($util.base64.length(object.eventId)), 0);
                                    else if (object.eventId.length)
                                        message.eventId = object.eventId;
                                if (object.insertTimestamp != null)
                                    if ($util.Long)
                                        (message.insertTimestamp = $util.Long.fromValue(object.insertTimestamp)).unsigned = false;
                                    else if (typeof object.insertTimestamp === "string")
                                        message.insertTimestamp = parseInt(object.insertTimestamp, 10);
                                    else if (typeof object.insertTimestamp === "number")
                                        message.insertTimestamp = object.insertTimestamp;
                                    else if (typeof object.insertTimestamp === "object")
                                        message.insertTimestamp = new $util.LongBits(object.insertTimestamp.low >>> 0, object.insertTimestamp.high >>> 0).toNumber();
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an InsertSuccessResponse message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.InsertSuccessResponse} message InsertSuccessResponse
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            InsertSuccessResponse.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    if (options.bytes === String)
                                        object.eventId = "";
                                    else {
                                        object.eventId = [];
                                        if (options.bytes !== Array)
                                            object.eventId = $util.newBuffer(object.eventId);
                                    }
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.insertTimestamp = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.insertTimestamp = options.longs === String ? "0" : 0;
                                }
                                if (message.eventId != null && message.hasOwnProperty("eventId"))
                                    object.eventId = options.bytes === String ? $util.base64.encode(message.eventId, 0, message.eventId.length) : options.bytes === Array ? Array.prototype.slice.call(message.eventId) : message.eventId;
                                if (message.insertTimestamp != null && message.hasOwnProperty("insertTimestamp"))
                                    if (typeof message.insertTimestamp === "number")
                                        object.insertTimestamp = options.longs === String ? String(message.insertTimestamp) : message.insertTimestamp;
                                    else
                                        object.insertTimestamp = options.longs === String ? $util.Long.prototype.toString.call(message.insertTimestamp) : options.longs === Number ? new $util.LongBits(message.insertTimestamp.low >>> 0, message.insertTimestamp.high >>> 0).toNumber() : message.insertTimestamp;
                                return object;
                            };
    
                            /**
                             * Converts this InsertSuccessResponse to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.InsertSuccessResponse
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            InsertSuccessResponse.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return InsertSuccessResponse;
                        })();
    
                        proto.InsertBulkRequest = (function() {
    
                            /**
                             * Properties of an InsertBulkRequest.
                             * @memberof com.isima.bios.models.proto
                             * @interface IInsertBulkRequest
                             * @property {com.isima.bios.models.proto.ContentRepresentation|null} [contentRep] InsertBulkRequest contentRep
                             * @property {Array.<com.isima.bios.models.proto.IRecord>|null} [record] InsertBulkRequest record
                             * @property {string|null} [signal] InsertBulkRequest signal
                             * @property {number|Long|null} [schemaVersion] InsertBulkRequest schemaVersion
                             */
    
                            /**
                             * Constructs a new InsertBulkRequest.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an InsertBulkRequest.
                             * @implements IInsertBulkRequest
                             * @constructor
                             * @param {com.isima.bios.models.proto.IInsertBulkRequest=} [properties] Properties to set
                             */
                            function InsertBulkRequest(properties) {
                                this.record = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * InsertBulkRequest contentRep.
                             * @member {com.isima.bios.models.proto.ContentRepresentation} contentRep
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @instance
                             */
                            InsertBulkRequest.prototype.contentRep = 0;
    
                            /**
                             * InsertBulkRequest record.
                             * @member {Array.<com.isima.bios.models.proto.IRecord>} record
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @instance
                             */
                            InsertBulkRequest.prototype.record = $util.emptyArray;
    
                            /**
                             * InsertBulkRequest signal.
                             * @member {string} signal
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @instance
                             */
                            InsertBulkRequest.prototype.signal = "";
    
                            /**
                             * InsertBulkRequest schemaVersion.
                             * @member {number|Long} schemaVersion
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @instance
                             */
                            InsertBulkRequest.prototype.schemaVersion = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * Creates a new InsertBulkRequest instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkRequest=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.InsertBulkRequest} InsertBulkRequest instance
                             */
                            InsertBulkRequest.create = function create(properties) {
                                return new InsertBulkRequest(properties);
                            };
    
                            /**
                             * Encodes the specified InsertBulkRequest message. Does not implicitly {@link com.isima.bios.models.proto.InsertBulkRequest.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkRequest} message InsertBulkRequest message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertBulkRequest.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.contentRep != null && Object.hasOwnProperty.call(message, "contentRep"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int32(message.contentRep);
                                if (message.record != null && message.record.length)
                                    for (var i = 0; i < message.record.length; ++i)
                                        $root.com.isima.bios.models.proto.Record.encode(message.record[i], writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
                                if (message.signal != null && Object.hasOwnProperty.call(message, "signal"))
                                    writer.uint32(/* id 5, wireType 2 =*/42).string(message.signal);
                                if (message.schemaVersion != null && Object.hasOwnProperty.call(message, "schemaVersion"))
                                    writer.uint32(/* id 10, wireType 0 =*/80).int64(message.schemaVersion);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified InsertBulkRequest message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.InsertBulkRequest.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkRequest} message InsertBulkRequest message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertBulkRequest.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an InsertBulkRequest message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.InsertBulkRequest} InsertBulkRequest
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertBulkRequest.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.InsertBulkRequest();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.contentRep = reader.int32();
                                        break;
                                    case 2:
                                        if (!(message.record && message.record.length))
                                            message.record = [];
                                        message.record.push($root.com.isima.bios.models.proto.Record.decode(reader, reader.uint32()));
                                        break;
                                    case 5:
                                        message.signal = reader.string();
                                        break;
                                    case 10:
                                        message.schemaVersion = reader.int64();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an InsertBulkRequest message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.InsertBulkRequest} InsertBulkRequest
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertBulkRequest.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an InsertBulkRequest message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            InsertBulkRequest.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.contentRep != null && message.hasOwnProperty("contentRep"))
                                    switch (message.contentRep) {
                                    default:
                                        return "contentRep: enum value expected";
                                    case 0:
                                    case 1:
                                    case 2:
                                        break;
                                    }
                                if (message.record != null && message.hasOwnProperty("record")) {
                                    if (!Array.isArray(message.record))
                                        return "record: array expected";
                                    for (var i = 0; i < message.record.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.Record.verify(message.record[i]);
                                        if (error)
                                            return "record." + error;
                                    }
                                }
                                if (message.signal != null && message.hasOwnProperty("signal"))
                                    if (!$util.isString(message.signal))
                                        return "signal: string expected";
                                if (message.schemaVersion != null && message.hasOwnProperty("schemaVersion"))
                                    if (!$util.isInteger(message.schemaVersion) && !(message.schemaVersion && $util.isInteger(message.schemaVersion.low) && $util.isInteger(message.schemaVersion.high)))
                                        return "schemaVersion: integer|Long expected";
                                return null;
                            };
    
                            /**
                             * Creates an InsertBulkRequest message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.InsertBulkRequest} InsertBulkRequest
                             */
                            InsertBulkRequest.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.InsertBulkRequest)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.InsertBulkRequest();
                                switch (object.contentRep) {
                                case "NATIVE":
                                case 0:
                                    message.contentRep = 0;
                                    break;
                                case "CSV":
                                case 1:
                                    message.contentRep = 1;
                                    break;
                                case "UNTYPED":
                                case 2:
                                    message.contentRep = 2;
                                    break;
                                }
                                if (object.record) {
                                    if (!Array.isArray(object.record))
                                        throw TypeError(".com.isima.bios.models.proto.InsertBulkRequest.record: array expected");
                                    message.record = [];
                                    for (var i = 0; i < object.record.length; ++i) {
                                        if (typeof object.record[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.InsertBulkRequest.record: object expected");
                                        message.record[i] = $root.com.isima.bios.models.proto.Record.fromObject(object.record[i]);
                                    }
                                }
                                if (object.signal != null)
                                    message.signal = String(object.signal);
                                if (object.schemaVersion != null)
                                    if ($util.Long)
                                        (message.schemaVersion = $util.Long.fromValue(object.schemaVersion)).unsigned = false;
                                    else if (typeof object.schemaVersion === "string")
                                        message.schemaVersion = parseInt(object.schemaVersion, 10);
                                    else if (typeof object.schemaVersion === "number")
                                        message.schemaVersion = object.schemaVersion;
                                    else if (typeof object.schemaVersion === "object")
                                        message.schemaVersion = new $util.LongBits(object.schemaVersion.low >>> 0, object.schemaVersion.high >>> 0).toNumber();
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an InsertBulkRequest message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @static
                             * @param {com.isima.bios.models.proto.InsertBulkRequest} message InsertBulkRequest
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            InsertBulkRequest.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.record = [];
                                if (options.defaults) {
                                    object.contentRep = options.enums === String ? "NATIVE" : 0;
                                    object.signal = "";
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.schemaVersion = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.schemaVersion = options.longs === String ? "0" : 0;
                                }
                                if (message.contentRep != null && message.hasOwnProperty("contentRep"))
                                    object.contentRep = options.enums === String ? $root.com.isima.bios.models.proto.ContentRepresentation[message.contentRep] : message.contentRep;
                                if (message.record && message.record.length) {
                                    object.record = [];
                                    for (var j = 0; j < message.record.length; ++j)
                                        object.record[j] = $root.com.isima.bios.models.proto.Record.toObject(message.record[j], options);
                                }
                                if (message.signal != null && message.hasOwnProperty("signal"))
                                    object.signal = message.signal;
                                if (message.schemaVersion != null && message.hasOwnProperty("schemaVersion"))
                                    if (typeof message.schemaVersion === "number")
                                        object.schemaVersion = options.longs === String ? String(message.schemaVersion) : message.schemaVersion;
                                    else
                                        object.schemaVersion = options.longs === String ? $util.Long.prototype.toString.call(message.schemaVersion) : options.longs === Number ? new $util.LongBits(message.schemaVersion.low >>> 0, message.schemaVersion.high >>> 0).toNumber() : message.schemaVersion;
                                return object;
                            };
    
                            /**
                             * Converts this InsertBulkRequest to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.InsertBulkRequest
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            InsertBulkRequest.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return InsertBulkRequest;
                        })();
    
                        proto.InsertBulkSuccessResponse = (function() {
    
                            /**
                             * Properties of an InsertBulkSuccessResponse.
                             * @memberof com.isima.bios.models.proto
                             * @interface IInsertBulkSuccessResponse
                             * @property {Array.<com.isima.bios.models.proto.IInsertSuccessResponse>|null} [responses] InsertBulkSuccessResponse responses
                             */
    
                            /**
                             * Constructs a new InsertBulkSuccessResponse.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an InsertBulkSuccessResponse.
                             * @implements IInsertBulkSuccessResponse
                             * @constructor
                             * @param {com.isima.bios.models.proto.IInsertBulkSuccessResponse=} [properties] Properties to set
                             */
                            function InsertBulkSuccessResponse(properties) {
                                this.responses = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * InsertBulkSuccessResponse responses.
                             * @member {Array.<com.isima.bios.models.proto.IInsertSuccessResponse>} responses
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @instance
                             */
                            InsertBulkSuccessResponse.prototype.responses = $util.emptyArray;
    
                            /**
                             * Creates a new InsertBulkSuccessResponse instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkSuccessResponse=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.InsertBulkSuccessResponse} InsertBulkSuccessResponse instance
                             */
                            InsertBulkSuccessResponse.create = function create(properties) {
                                return new InsertBulkSuccessResponse(properties);
                            };
    
                            /**
                             * Encodes the specified InsertBulkSuccessResponse message. Does not implicitly {@link com.isima.bios.models.proto.InsertBulkSuccessResponse.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkSuccessResponse} message InsertBulkSuccessResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertBulkSuccessResponse.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.responses != null && message.responses.length)
                                    for (var i = 0; i < message.responses.length; ++i)
                                        $root.com.isima.bios.models.proto.InsertSuccessResponse.encode(message.responses[i], writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
                                return writer;
                            };
    
                            /**
                             * Encodes the specified InsertBulkSuccessResponse message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.InsertBulkSuccessResponse.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkSuccessResponse} message InsertBulkSuccessResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertBulkSuccessResponse.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an InsertBulkSuccessResponse message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.InsertBulkSuccessResponse} InsertBulkSuccessResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertBulkSuccessResponse.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.InsertBulkSuccessResponse();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        if (!(message.responses && message.responses.length))
                                            message.responses = [];
                                        message.responses.push($root.com.isima.bios.models.proto.InsertSuccessResponse.decode(reader, reader.uint32()));
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an InsertBulkSuccessResponse message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.InsertBulkSuccessResponse} InsertBulkSuccessResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertBulkSuccessResponse.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an InsertBulkSuccessResponse message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            InsertBulkSuccessResponse.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.responses != null && message.hasOwnProperty("responses")) {
                                    if (!Array.isArray(message.responses))
                                        return "responses: array expected";
                                    for (var i = 0; i < message.responses.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.InsertSuccessResponse.verify(message.responses[i]);
                                        if (error)
                                            return "responses." + error;
                                    }
                                }
                                return null;
                            };
    
                            /**
                             * Creates an InsertBulkSuccessResponse message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.InsertBulkSuccessResponse} InsertBulkSuccessResponse
                             */
                            InsertBulkSuccessResponse.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.InsertBulkSuccessResponse)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.InsertBulkSuccessResponse();
                                if (object.responses) {
                                    if (!Array.isArray(object.responses))
                                        throw TypeError(".com.isima.bios.models.proto.InsertBulkSuccessResponse.responses: array expected");
                                    message.responses = [];
                                    for (var i = 0; i < object.responses.length; ++i) {
                                        if (typeof object.responses[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.InsertBulkSuccessResponse.responses: object expected");
                                        message.responses[i] = $root.com.isima.bios.models.proto.InsertSuccessResponse.fromObject(object.responses[i]);
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an InsertBulkSuccessResponse message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @static
                             * @param {com.isima.bios.models.proto.InsertBulkSuccessResponse} message InsertBulkSuccessResponse
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            InsertBulkSuccessResponse.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.responses = [];
                                if (message.responses && message.responses.length) {
                                    object.responses = [];
                                    for (var j = 0; j < message.responses.length; ++j)
                                        object.responses[j] = $root.com.isima.bios.models.proto.InsertSuccessResponse.toObject(message.responses[j], options);
                                }
                                return object;
                            };
    
                            /**
                             * Converts this InsertBulkSuccessResponse to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.InsertBulkSuccessResponse
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            InsertBulkSuccessResponse.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return InsertBulkSuccessResponse;
                        })();
    
                        proto.InsertSuccessOrError = (function() {
    
                            /**
                             * Properties of an InsertSuccessOrError.
                             * @memberof com.isima.bios.models.proto
                             * @interface IInsertSuccessOrError
                             * @property {string|null} [eventId] InsertSuccessOrError eventId
                             * @property {number|Long|null} [insertTimestamp] InsertSuccessOrError insertTimestamp
                             * @property {string|null} [errorMessage] InsertSuccessOrError errorMessage
                             * @property {string|null} [serverErrorCode] InsertSuccessOrError serverErrorCode
                             */
    
                            /**
                             * Constructs a new InsertSuccessOrError.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an InsertSuccessOrError.
                             * @implements IInsertSuccessOrError
                             * @constructor
                             * @param {com.isima.bios.models.proto.IInsertSuccessOrError=} [properties] Properties to set
                             */
                            function InsertSuccessOrError(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * InsertSuccessOrError eventId.
                             * @member {string} eventId
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @instance
                             */
                            InsertSuccessOrError.prototype.eventId = "";
    
                            /**
                             * InsertSuccessOrError insertTimestamp.
                             * @member {number|Long} insertTimestamp
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @instance
                             */
                            InsertSuccessOrError.prototype.insertTimestamp = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * InsertSuccessOrError errorMessage.
                             * @member {string} errorMessage
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @instance
                             */
                            InsertSuccessOrError.prototype.errorMessage = "";
    
                            /**
                             * InsertSuccessOrError serverErrorCode.
                             * @member {string} serverErrorCode
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @instance
                             */
                            InsertSuccessOrError.prototype.serverErrorCode = "";
    
                            /**
                             * Creates a new InsertSuccessOrError instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertSuccessOrError=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.InsertSuccessOrError} InsertSuccessOrError instance
                             */
                            InsertSuccessOrError.create = function create(properties) {
                                return new InsertSuccessOrError(properties);
                            };
    
                            /**
                             * Encodes the specified InsertSuccessOrError message. Does not implicitly {@link com.isima.bios.models.proto.InsertSuccessOrError.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertSuccessOrError} message InsertSuccessOrError message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertSuccessOrError.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.eventId != null && Object.hasOwnProperty.call(message, "eventId"))
                                    writer.uint32(/* id 1, wireType 2 =*/10).string(message.eventId);
                                if (message.insertTimestamp != null && Object.hasOwnProperty.call(message, "insertTimestamp"))
                                    writer.uint32(/* id 2, wireType 0 =*/16).int64(message.insertTimestamp);
                                if (message.errorMessage != null && Object.hasOwnProperty.call(message, "errorMessage"))
                                    writer.uint32(/* id 10, wireType 2 =*/82).string(message.errorMessage);
                                if (message.serverErrorCode != null && Object.hasOwnProperty.call(message, "serverErrorCode"))
                                    writer.uint32(/* id 11, wireType 2 =*/90).string(message.serverErrorCode);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified InsertSuccessOrError message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.InsertSuccessOrError.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertSuccessOrError} message InsertSuccessOrError message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertSuccessOrError.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an InsertSuccessOrError message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.InsertSuccessOrError} InsertSuccessOrError
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertSuccessOrError.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.InsertSuccessOrError();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.eventId = reader.string();
                                        break;
                                    case 2:
                                        message.insertTimestamp = reader.int64();
                                        break;
                                    case 10:
                                        message.errorMessage = reader.string();
                                        break;
                                    case 11:
                                        message.serverErrorCode = reader.string();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an InsertSuccessOrError message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.InsertSuccessOrError} InsertSuccessOrError
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertSuccessOrError.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an InsertSuccessOrError message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            InsertSuccessOrError.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.eventId != null && message.hasOwnProperty("eventId"))
                                    if (!$util.isString(message.eventId))
                                        return "eventId: string expected";
                                if (message.insertTimestamp != null && message.hasOwnProperty("insertTimestamp"))
                                    if (!$util.isInteger(message.insertTimestamp) && !(message.insertTimestamp && $util.isInteger(message.insertTimestamp.low) && $util.isInteger(message.insertTimestamp.high)))
                                        return "insertTimestamp: integer|Long expected";
                                if (message.errorMessage != null && message.hasOwnProperty("errorMessage"))
                                    if (!$util.isString(message.errorMessage))
                                        return "errorMessage: string expected";
                                if (message.serverErrorCode != null && message.hasOwnProperty("serverErrorCode"))
                                    if (!$util.isString(message.serverErrorCode))
                                        return "serverErrorCode: string expected";
                                return null;
                            };
    
                            /**
                             * Creates an InsertSuccessOrError message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.InsertSuccessOrError} InsertSuccessOrError
                             */
                            InsertSuccessOrError.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.InsertSuccessOrError)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.InsertSuccessOrError();
                                if (object.eventId != null)
                                    message.eventId = String(object.eventId);
                                if (object.insertTimestamp != null)
                                    if ($util.Long)
                                        (message.insertTimestamp = $util.Long.fromValue(object.insertTimestamp)).unsigned = false;
                                    else if (typeof object.insertTimestamp === "string")
                                        message.insertTimestamp = parseInt(object.insertTimestamp, 10);
                                    else if (typeof object.insertTimestamp === "number")
                                        message.insertTimestamp = object.insertTimestamp;
                                    else if (typeof object.insertTimestamp === "object")
                                        message.insertTimestamp = new $util.LongBits(object.insertTimestamp.low >>> 0, object.insertTimestamp.high >>> 0).toNumber();
                                if (object.errorMessage != null)
                                    message.errorMessage = String(object.errorMessage);
                                if (object.serverErrorCode != null)
                                    message.serverErrorCode = String(object.serverErrorCode);
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an InsertSuccessOrError message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @static
                             * @param {com.isima.bios.models.proto.InsertSuccessOrError} message InsertSuccessOrError
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            InsertSuccessOrError.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    object.eventId = "";
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.insertTimestamp = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.insertTimestamp = options.longs === String ? "0" : 0;
                                    object.errorMessage = "";
                                    object.serverErrorCode = "";
                                }
                                if (message.eventId != null && message.hasOwnProperty("eventId"))
                                    object.eventId = message.eventId;
                                if (message.insertTimestamp != null && message.hasOwnProperty("insertTimestamp"))
                                    if (typeof message.insertTimestamp === "number")
                                        object.insertTimestamp = options.longs === String ? String(message.insertTimestamp) : message.insertTimestamp;
                                    else
                                        object.insertTimestamp = options.longs === String ? $util.Long.prototype.toString.call(message.insertTimestamp) : options.longs === Number ? new $util.LongBits(message.insertTimestamp.low >>> 0, message.insertTimestamp.high >>> 0).toNumber() : message.insertTimestamp;
                                if (message.errorMessage != null && message.hasOwnProperty("errorMessage"))
                                    object.errorMessage = message.errorMessage;
                                if (message.serverErrorCode != null && message.hasOwnProperty("serverErrorCode"))
                                    object.serverErrorCode = message.serverErrorCode;
                                return object;
                            };
    
                            /**
                             * Converts this InsertSuccessOrError to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.InsertSuccessOrError
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            InsertSuccessOrError.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return InsertSuccessOrError;
                        })();
    
                        proto.InsertBulkErrorResponse = (function() {
    
                            /**
                             * Properties of an InsertBulkErrorResponse.
                             * @memberof com.isima.bios.models.proto
                             * @interface IInsertBulkErrorResponse
                             * @property {string|null} [serverErrorCode] InsertBulkErrorResponse serverErrorCode
                             * @property {string|null} [serverErrorMessage] InsertBulkErrorResponse serverErrorMessage
                             * @property {Array.<com.isima.bios.models.proto.IInsertSuccessOrError>|null} [resultsWithError] InsertBulkErrorResponse resultsWithError
                             */
    
                            /**
                             * Constructs a new InsertBulkErrorResponse.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents an InsertBulkErrorResponse.
                             * @implements IInsertBulkErrorResponse
                             * @constructor
                             * @param {com.isima.bios.models.proto.IInsertBulkErrorResponse=} [properties] Properties to set
                             */
                            function InsertBulkErrorResponse(properties) {
                                this.resultsWithError = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * InsertBulkErrorResponse serverErrorCode.
                             * @member {string} serverErrorCode
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @instance
                             */
                            InsertBulkErrorResponse.prototype.serverErrorCode = "";
    
                            /**
                             * InsertBulkErrorResponse serverErrorMessage.
                             * @member {string} serverErrorMessage
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @instance
                             */
                            InsertBulkErrorResponse.prototype.serverErrorMessage = "";
    
                            /**
                             * InsertBulkErrorResponse resultsWithError.
                             * @member {Array.<com.isima.bios.models.proto.IInsertSuccessOrError>} resultsWithError
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @instance
                             */
                            InsertBulkErrorResponse.prototype.resultsWithError = $util.emptyArray;
    
                            /**
                             * Creates a new InsertBulkErrorResponse instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkErrorResponse=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.InsertBulkErrorResponse} InsertBulkErrorResponse instance
                             */
                            InsertBulkErrorResponse.create = function create(properties) {
                                return new InsertBulkErrorResponse(properties);
                            };
    
                            /**
                             * Encodes the specified InsertBulkErrorResponse message. Does not implicitly {@link com.isima.bios.models.proto.InsertBulkErrorResponse.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkErrorResponse} message InsertBulkErrorResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertBulkErrorResponse.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.serverErrorCode != null && Object.hasOwnProperty.call(message, "serverErrorCode"))
                                    writer.uint32(/* id 1, wireType 2 =*/10).string(message.serverErrorCode);
                                if (message.serverErrorMessage != null && Object.hasOwnProperty.call(message, "serverErrorMessage"))
                                    writer.uint32(/* id 2, wireType 2 =*/18).string(message.serverErrorMessage);
                                if (message.resultsWithError != null && message.resultsWithError.length)
                                    for (var i = 0; i < message.resultsWithError.length; ++i)
                                        $root.com.isima.bios.models.proto.InsertSuccessOrError.encode(message.resultsWithError[i], writer.uint32(/* id 4, wireType 2 =*/34).fork()).ldelim();
                                return writer;
                            };
    
                            /**
                             * Encodes the specified InsertBulkErrorResponse message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.InsertBulkErrorResponse.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {com.isima.bios.models.proto.IInsertBulkErrorResponse} message InsertBulkErrorResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            InsertBulkErrorResponse.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes an InsertBulkErrorResponse message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.InsertBulkErrorResponse} InsertBulkErrorResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertBulkErrorResponse.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.InsertBulkErrorResponse();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.serverErrorCode = reader.string();
                                        break;
                                    case 2:
                                        message.serverErrorMessage = reader.string();
                                        break;
                                    case 4:
                                        if (!(message.resultsWithError && message.resultsWithError.length))
                                            message.resultsWithError = [];
                                        message.resultsWithError.push($root.com.isima.bios.models.proto.InsertSuccessOrError.decode(reader, reader.uint32()));
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes an InsertBulkErrorResponse message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.InsertBulkErrorResponse} InsertBulkErrorResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            InsertBulkErrorResponse.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies an InsertBulkErrorResponse message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            InsertBulkErrorResponse.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.serverErrorCode != null && message.hasOwnProperty("serverErrorCode"))
                                    if (!$util.isString(message.serverErrorCode))
                                        return "serverErrorCode: string expected";
                                if (message.serverErrorMessage != null && message.hasOwnProperty("serverErrorMessage"))
                                    if (!$util.isString(message.serverErrorMessage))
                                        return "serverErrorMessage: string expected";
                                if (message.resultsWithError != null && message.hasOwnProperty("resultsWithError")) {
                                    if (!Array.isArray(message.resultsWithError))
                                        return "resultsWithError: array expected";
                                    for (var i = 0; i < message.resultsWithError.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.InsertSuccessOrError.verify(message.resultsWithError[i]);
                                        if (error)
                                            return "resultsWithError." + error;
                                    }
                                }
                                return null;
                            };
    
                            /**
                             * Creates an InsertBulkErrorResponse message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.InsertBulkErrorResponse} InsertBulkErrorResponse
                             */
                            InsertBulkErrorResponse.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.InsertBulkErrorResponse)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.InsertBulkErrorResponse();
                                if (object.serverErrorCode != null)
                                    message.serverErrorCode = String(object.serverErrorCode);
                                if (object.serverErrorMessage != null)
                                    message.serverErrorMessage = String(object.serverErrorMessage);
                                if (object.resultsWithError) {
                                    if (!Array.isArray(object.resultsWithError))
                                        throw TypeError(".com.isima.bios.models.proto.InsertBulkErrorResponse.resultsWithError: array expected");
                                    message.resultsWithError = [];
                                    for (var i = 0; i < object.resultsWithError.length; ++i) {
                                        if (typeof object.resultsWithError[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.InsertBulkErrorResponse.resultsWithError: object expected");
                                        message.resultsWithError[i] = $root.com.isima.bios.models.proto.InsertSuccessOrError.fromObject(object.resultsWithError[i]);
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from an InsertBulkErrorResponse message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @static
                             * @param {com.isima.bios.models.proto.InsertBulkErrorResponse} message InsertBulkErrorResponse
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            InsertBulkErrorResponse.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.resultsWithError = [];
                                if (options.defaults) {
                                    object.serverErrorCode = "";
                                    object.serverErrorMessage = "";
                                }
                                if (message.serverErrorCode != null && message.hasOwnProperty("serverErrorCode"))
                                    object.serverErrorCode = message.serverErrorCode;
                                if (message.serverErrorMessage != null && message.hasOwnProperty("serverErrorMessage"))
                                    object.serverErrorMessage = message.serverErrorMessage;
                                if (message.resultsWithError && message.resultsWithError.length) {
                                    object.resultsWithError = [];
                                    for (var j = 0; j < message.resultsWithError.length; ++j)
                                        object.resultsWithError[j] = $root.com.isima.bios.models.proto.InsertSuccessOrError.toObject(message.resultsWithError[j], options);
                                }
                                return object;
                            };
    
                            /**
                             * Converts this InsertBulkErrorResponse to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.InsertBulkErrorResponse
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            InsertBulkErrorResponse.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return InsertBulkErrorResponse;
                        })();
    
                        /**
                         * WindowType enum.
                         * @name com.isima.bios.models.proto.WindowType
                         * @enum {number}
                         * @property {number} GLOBAL_WINDOW=0 GLOBAL_WINDOW value
                         * @property {number} SLIDING_WINDOW=1 SLIDING_WINDOW value
                         * @property {number} TUMBLING_WINDOW=2 TUMBLING_WINDOW value
                         */
                        proto.WindowType = (function() {
                            var valuesById = {}, values = Object.create(valuesById);
                            values[valuesById[0] = "GLOBAL_WINDOW"] = 0;
                            values[valuesById[1] = "SLIDING_WINDOW"] = 1;
                            values[valuesById[2] = "TUMBLING_WINDOW"] = 2;
                            return values;
                        })();
    
                        proto.SlidingWindow = (function() {
    
                            /**
                             * Properties of a SlidingWindow.
                             * @memberof com.isima.bios.models.proto
                             * @interface ISlidingWindow
                             * @property {number|Long|null} [slideInterval] SlidingWindow slideInterval
                             * @property {number|null} [windowSlides] SlidingWindow windowSlides
                             */
    
                            /**
                             * Constructs a new SlidingWindow.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a SlidingWindow.
                             * @implements ISlidingWindow
                             * @constructor
                             * @param {com.isima.bios.models.proto.ISlidingWindow=} [properties] Properties to set
                             */
                            function SlidingWindow(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * SlidingWindow slideInterval.
                             * @member {number|Long} slideInterval
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @instance
                             */
                            SlidingWindow.prototype.slideInterval = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * SlidingWindow windowSlides.
                             * @member {number} windowSlides
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @instance
                             */
                            SlidingWindow.prototype.windowSlides = 0;
    
                            /**
                             * Creates a new SlidingWindow instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.ISlidingWindow=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.SlidingWindow} SlidingWindow instance
                             */
                            SlidingWindow.create = function create(properties) {
                                return new SlidingWindow(properties);
                            };
    
                            /**
                             * Encodes the specified SlidingWindow message. Does not implicitly {@link com.isima.bios.models.proto.SlidingWindow.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.ISlidingWindow} message SlidingWindow message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SlidingWindow.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.slideInterval != null && Object.hasOwnProperty.call(message, "slideInterval"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int64(message.slideInterval);
                                if (message.windowSlides != null && Object.hasOwnProperty.call(message, "windowSlides"))
                                    writer.uint32(/* id 2, wireType 0 =*/16).int32(message.windowSlides);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified SlidingWindow message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.SlidingWindow.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.ISlidingWindow} message SlidingWindow message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SlidingWindow.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a SlidingWindow message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.SlidingWindow} SlidingWindow
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SlidingWindow.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.SlidingWindow();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.slideInterval = reader.int64();
                                        break;
                                    case 2:
                                        message.windowSlides = reader.int32();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a SlidingWindow message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.SlidingWindow} SlidingWindow
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SlidingWindow.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a SlidingWindow message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            SlidingWindow.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.slideInterval != null && message.hasOwnProperty("slideInterval"))
                                    if (!$util.isInteger(message.slideInterval) && !(message.slideInterval && $util.isInteger(message.slideInterval.low) && $util.isInteger(message.slideInterval.high)))
                                        return "slideInterval: integer|Long expected";
                                if (message.windowSlides != null && message.hasOwnProperty("windowSlides"))
                                    if (!$util.isInteger(message.windowSlides))
                                        return "windowSlides: integer expected";
                                return null;
                            };
    
                            /**
                             * Creates a SlidingWindow message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.SlidingWindow} SlidingWindow
                             */
                            SlidingWindow.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.SlidingWindow)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.SlidingWindow();
                                if (object.slideInterval != null)
                                    if ($util.Long)
                                        (message.slideInterval = $util.Long.fromValue(object.slideInterval)).unsigned = false;
                                    else if (typeof object.slideInterval === "string")
                                        message.slideInterval = parseInt(object.slideInterval, 10);
                                    else if (typeof object.slideInterval === "number")
                                        message.slideInterval = object.slideInterval;
                                    else if (typeof object.slideInterval === "object")
                                        message.slideInterval = new $util.LongBits(object.slideInterval.low >>> 0, object.slideInterval.high >>> 0).toNumber();
                                if (object.windowSlides != null)
                                    message.windowSlides = object.windowSlides | 0;
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a SlidingWindow message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.SlidingWindow} message SlidingWindow
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            SlidingWindow.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults) {
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.slideInterval = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.slideInterval = options.longs === String ? "0" : 0;
                                    object.windowSlides = 0;
                                }
                                if (message.slideInterval != null && message.hasOwnProperty("slideInterval"))
                                    if (typeof message.slideInterval === "number")
                                        object.slideInterval = options.longs === String ? String(message.slideInterval) : message.slideInterval;
                                    else
                                        object.slideInterval = options.longs === String ? $util.Long.prototype.toString.call(message.slideInterval) : options.longs === Number ? new $util.LongBits(message.slideInterval.low >>> 0, message.slideInterval.high >>> 0).toNumber() : message.slideInterval;
                                if (message.windowSlides != null && message.hasOwnProperty("windowSlides"))
                                    object.windowSlides = message.windowSlides;
                                return object;
                            };
    
                            /**
                             * Converts this SlidingWindow to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.SlidingWindow
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            SlidingWindow.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return SlidingWindow;
                        })();
    
                        proto.TumblingWindow = (function() {
    
                            /**
                             * Properties of a TumblingWindow.
                             * @memberof com.isima.bios.models.proto
                             * @interface ITumblingWindow
                             * @property {number|Long|null} [windowSizeMs] TumblingWindow windowSizeMs
                             */
    
                            /**
                             * Constructs a new TumblingWindow.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a TumblingWindow.
                             * @implements ITumblingWindow
                             * @constructor
                             * @param {com.isima.bios.models.proto.ITumblingWindow=} [properties] Properties to set
                             */
                            function TumblingWindow(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * TumblingWindow windowSizeMs.
                             * @member {number|Long} windowSizeMs
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @instance
                             */
                            TumblingWindow.prototype.windowSizeMs = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * Creates a new TumblingWindow instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.ITumblingWindow=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.TumblingWindow} TumblingWindow instance
                             */
                            TumblingWindow.create = function create(properties) {
                                return new TumblingWindow(properties);
                            };
    
                            /**
                             * Encodes the specified TumblingWindow message. Does not implicitly {@link com.isima.bios.models.proto.TumblingWindow.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.ITumblingWindow} message TumblingWindow message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            TumblingWindow.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.windowSizeMs != null && Object.hasOwnProperty.call(message, "windowSizeMs"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int64(message.windowSizeMs);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified TumblingWindow message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.TumblingWindow.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.ITumblingWindow} message TumblingWindow message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            TumblingWindow.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a TumblingWindow message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.TumblingWindow} TumblingWindow
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            TumblingWindow.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.TumblingWindow();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.windowSizeMs = reader.int64();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a TumblingWindow message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.TumblingWindow} TumblingWindow
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            TumblingWindow.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a TumblingWindow message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            TumblingWindow.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.windowSizeMs != null && message.hasOwnProperty("windowSizeMs"))
                                    if (!$util.isInteger(message.windowSizeMs) && !(message.windowSizeMs && $util.isInteger(message.windowSizeMs.low) && $util.isInteger(message.windowSizeMs.high)))
                                        return "windowSizeMs: integer|Long expected";
                                return null;
                            };
    
                            /**
                             * Creates a TumblingWindow message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.TumblingWindow} TumblingWindow
                             */
                            TumblingWindow.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.TumblingWindow)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.TumblingWindow();
                                if (object.windowSizeMs != null)
                                    if ($util.Long)
                                        (message.windowSizeMs = $util.Long.fromValue(object.windowSizeMs)).unsigned = false;
                                    else if (typeof object.windowSizeMs === "string")
                                        message.windowSizeMs = parseInt(object.windowSizeMs, 10);
                                    else if (typeof object.windowSizeMs === "number")
                                        message.windowSizeMs = object.windowSizeMs;
                                    else if (typeof object.windowSizeMs === "object")
                                        message.windowSizeMs = new $util.LongBits(object.windowSizeMs.low >>> 0, object.windowSizeMs.high >>> 0).toNumber();
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a TumblingWindow message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @static
                             * @param {com.isima.bios.models.proto.TumblingWindow} message TumblingWindow
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            TumblingWindow.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults)
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.windowSizeMs = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.windowSizeMs = options.longs === String ? "0" : 0;
                                if (message.windowSizeMs != null && message.hasOwnProperty("windowSizeMs"))
                                    if (typeof message.windowSizeMs === "number")
                                        object.windowSizeMs = options.longs === String ? String(message.windowSizeMs) : message.windowSizeMs;
                                    else
                                        object.windowSizeMs = options.longs === String ? $util.Long.prototype.toString.call(message.windowSizeMs) : options.longs === Number ? new $util.LongBits(message.windowSizeMs.low >>> 0, message.windowSizeMs.high >>> 0).toNumber() : message.windowSizeMs;
                                return object;
                            };
    
                            /**
                             * Converts this TumblingWindow to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.TumblingWindow
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            TumblingWindow.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return TumblingWindow;
                        })();
    
                        proto.Window = (function() {
    
                            /**
                             * Properties of a Window.
                             * @memberof com.isima.bios.models.proto
                             * @interface IWindow
                             * @property {com.isima.bios.models.proto.WindowType|null} [windowType] Window windowType
                             * @property {com.isima.bios.models.proto.ISlidingWindow|null} [sliding] Window sliding
                             * @property {com.isima.bios.models.proto.ITumblingWindow|null} [tumbling] Window tumbling
                             */
    
                            /**
                             * Constructs a new Window.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a Window.
                             * @implements IWindow
                             * @constructor
                             * @param {com.isima.bios.models.proto.IWindow=} [properties] Properties to set
                             */
                            function Window(properties) {
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * Window windowType.
                             * @member {com.isima.bios.models.proto.WindowType} windowType
                             * @memberof com.isima.bios.models.proto.Window
                             * @instance
                             */
                            Window.prototype.windowType = 0;
    
                            /**
                             * Window sliding.
                             * @member {com.isima.bios.models.proto.ISlidingWindow|null|undefined} sliding
                             * @memberof com.isima.bios.models.proto.Window
                             * @instance
                             */
                            Window.prototype.sliding = null;
    
                            /**
                             * Window tumbling.
                             * @member {com.isima.bios.models.proto.ITumblingWindow|null|undefined} tumbling
                             * @memberof com.isima.bios.models.proto.Window
                             * @instance
                             */
                            Window.prototype.tumbling = null;
    
                            // OneOf field names bound to virtual getters and setters
                            var $oneOfFields;
    
                            /**
                             * Window windowDetails.
                             * @member {"sliding"|"tumbling"|undefined} windowDetails
                             * @memberof com.isima.bios.models.proto.Window
                             * @instance
                             */
                            Object.defineProperty(Window.prototype, "windowDetails", {
                                get: $util.oneOfGetter($oneOfFields = ["sliding", "tumbling"]),
                                set: $util.oneOfSetter($oneOfFields)
                            });
    
                            /**
                             * Creates a new Window instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {com.isima.bios.models.proto.IWindow=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.Window} Window instance
                             */
                            Window.create = function create(properties) {
                                return new Window(properties);
                            };
    
                            /**
                             * Encodes the specified Window message. Does not implicitly {@link com.isima.bios.models.proto.Window.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {com.isima.bios.models.proto.IWindow} message Window message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Window.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.windowType != null && Object.hasOwnProperty.call(message, "windowType"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int32(message.windowType);
                                if (message.sliding != null && Object.hasOwnProperty.call(message, "sliding"))
                                    $root.com.isima.bios.models.proto.SlidingWindow.encode(message.sliding, writer.uint32(/* id 10, wireType 2 =*/82).fork()).ldelim();
                                if (message.tumbling != null && Object.hasOwnProperty.call(message, "tumbling"))
                                    $root.com.isima.bios.models.proto.TumblingWindow.encode(message.tumbling, writer.uint32(/* id 11, wireType 2 =*/90).fork()).ldelim();
                                return writer;
                            };
    
                            /**
                             * Encodes the specified Window message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.Window.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {com.isima.bios.models.proto.IWindow} message Window message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            Window.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a Window message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.Window} Window
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Window.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.Window();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.windowType = reader.int32();
                                        break;
                                    case 10:
                                        message.sliding = $root.com.isima.bios.models.proto.SlidingWindow.decode(reader, reader.uint32());
                                        break;
                                    case 11:
                                        message.tumbling = $root.com.isima.bios.models.proto.TumblingWindow.decode(reader, reader.uint32());
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a Window message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.Window} Window
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            Window.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a Window message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            Window.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                var properties = {};
                                if (message.windowType != null && message.hasOwnProperty("windowType"))
                                    switch (message.windowType) {
                                    default:
                                        return "windowType: enum value expected";
                                    case 0:
                                    case 1:
                                    case 2:
                                        break;
                                    }
                                if (message.sliding != null && message.hasOwnProperty("sliding")) {
                                    properties.windowDetails = 1;
                                    {
                                        var error = $root.com.isima.bios.models.proto.SlidingWindow.verify(message.sliding);
                                        if (error)
                                            return "sliding." + error;
                                    }
                                }
                                if (message.tumbling != null && message.hasOwnProperty("tumbling")) {
                                    if (properties.windowDetails === 1)
                                        return "windowDetails: multiple values";
                                    properties.windowDetails = 1;
                                    {
                                        var error = $root.com.isima.bios.models.proto.TumblingWindow.verify(message.tumbling);
                                        if (error)
                                            return "tumbling." + error;
                                    }
                                }
                                return null;
                            };
    
                            /**
                             * Creates a Window message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.Window} Window
                             */
                            Window.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.Window)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.Window();
                                switch (object.windowType) {
                                case "GLOBAL_WINDOW":
                                case 0:
                                    message.windowType = 0;
                                    break;
                                case "SLIDING_WINDOW":
                                case 1:
                                    message.windowType = 1;
                                    break;
                                case "TUMBLING_WINDOW":
                                case 2:
                                    message.windowType = 2;
                                    break;
                                }
                                if (object.sliding != null) {
                                    if (typeof object.sliding !== "object")
                                        throw TypeError(".com.isima.bios.models.proto.Window.sliding: object expected");
                                    message.sliding = $root.com.isima.bios.models.proto.SlidingWindow.fromObject(object.sliding);
                                }
                                if (object.tumbling != null) {
                                    if (typeof object.tumbling !== "object")
                                        throw TypeError(".com.isima.bios.models.proto.Window.tumbling: object expected");
                                    message.tumbling = $root.com.isima.bios.models.proto.TumblingWindow.fromObject(object.tumbling);
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a Window message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.Window
                             * @static
                             * @param {com.isima.bios.models.proto.Window} message Window
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            Window.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.defaults)
                                    object.windowType = options.enums === String ? "GLOBAL_WINDOW" : 0;
                                if (message.windowType != null && message.hasOwnProperty("windowType"))
                                    object.windowType = options.enums === String ? $root.com.isima.bios.models.proto.WindowType[message.windowType] : message.windowType;
                                if (message.sliding != null && message.hasOwnProperty("sliding")) {
                                    object.sliding = $root.com.isima.bios.models.proto.SlidingWindow.toObject(message.sliding, options);
                                    if (options.oneofs)
                                        object.windowDetails = "sliding";
                                }
                                if (message.tumbling != null && message.hasOwnProperty("tumbling")) {
                                    object.tumbling = $root.com.isima.bios.models.proto.TumblingWindow.toObject(message.tumbling, options);
                                    if (options.oneofs)
                                        object.windowDetails = "tumbling";
                                }
                                return object;
                            };
    
                            /**
                             * Converts this Window to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.Window
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            Window.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return Window;
                        })();
    
                        proto.SelectQuery = (function() {
    
                            /**
                             * Properties of a SelectQuery.
                             * @memberof com.isima.bios.models.proto
                             * @interface ISelectQuery
                             * @property {number|Long|null} [startTime] SelectQuery startTime
                             * @property {number|Long|null} [endTime] SelectQuery endTime
                             * @property {boolean|null} [distinct] SelectQuery distinct
                             * @property {com.isima.bios.models.proto.IAttributeList|null} [attributes] SelectQuery attributes
                             * @property {Array.<com.isima.bios.models.proto.IMetric>|null} [metrics] SelectQuery metrics
                             * @property {string|null} [from] SelectQuery from
                             * @property {string|null} [where] SelectQuery where
                             * @property {com.isima.bios.models.proto.IDimensions|null} [groupBy] SelectQuery groupBy
                             * @property {Array.<com.isima.bios.models.proto.IWindow>|null} [windows] SelectQuery windows
                             * @property {com.isima.bios.models.proto.IOrderBy|null} [orderBy] SelectQuery orderBy
                             * @property {number|null} [limit] SelectQuery limit
                             * @property {boolean|null} [onTheFly] SelectQuery onTheFly
                             */
    
                            /**
                             * Constructs a new SelectQuery.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a SelectQuery.
                             * @implements ISelectQuery
                             * @constructor
                             * @param {com.isima.bios.models.proto.ISelectQuery=} [properties] Properties to set
                             */
                            function SelectQuery(properties) {
                                this.metrics = [];
                                this.windows = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * SelectQuery startTime.
                             * @member {number|Long} startTime
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.startTime = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * SelectQuery endTime.
                             * @member {number|Long} endTime
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.endTime = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * SelectQuery distinct.
                             * @member {boolean} distinct
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.distinct = false;
    
                            /**
                             * SelectQuery attributes.
                             * @member {com.isima.bios.models.proto.IAttributeList|null|undefined} attributes
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.attributes = null;
    
                            /**
                             * SelectQuery metrics.
                             * @member {Array.<com.isima.bios.models.proto.IMetric>} metrics
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.metrics = $util.emptyArray;
    
                            /**
                             * SelectQuery from.
                             * @member {string} from
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.from = "";
    
                            /**
                             * SelectQuery where.
                             * @member {string} where
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.where = "";
    
                            /**
                             * SelectQuery groupBy.
                             * @member {com.isima.bios.models.proto.IDimensions|null|undefined} groupBy
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.groupBy = null;
    
                            /**
                             * SelectQuery windows.
                             * @member {Array.<com.isima.bios.models.proto.IWindow>} windows
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.windows = $util.emptyArray;
    
                            /**
                             * SelectQuery orderBy.
                             * @member {com.isima.bios.models.proto.IOrderBy|null|undefined} orderBy
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.orderBy = null;
    
                            /**
                             * SelectQuery limit.
                             * @member {number} limit
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.limit = 0;
    
                            /**
                             * SelectQuery onTheFly.
                             * @member {boolean} onTheFly
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             */
                            SelectQuery.prototype.onTheFly = false;
    
                            /**
                             * Creates a new SelectQuery instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectQuery=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.SelectQuery} SelectQuery instance
                             */
                            SelectQuery.create = function create(properties) {
                                return new SelectQuery(properties);
                            };
    
                            /**
                             * Encodes the specified SelectQuery message. Does not implicitly {@link com.isima.bios.models.proto.SelectQuery.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectQuery} message SelectQuery message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectQuery.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.startTime != null && Object.hasOwnProperty.call(message, "startTime"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int64(message.startTime);
                                if (message.endTime != null && Object.hasOwnProperty.call(message, "endTime"))
                                    writer.uint32(/* id 2, wireType 0 =*/16).int64(message.endTime);
                                if (message.distinct != null && Object.hasOwnProperty.call(message, "distinct"))
                                    writer.uint32(/* id 10, wireType 0 =*/80).bool(message.distinct);
                                if (message.attributes != null && Object.hasOwnProperty.call(message, "attributes"))
                                    $root.com.isima.bios.models.proto.AttributeList.encode(message.attributes, writer.uint32(/* id 11, wireType 2 =*/90).fork()).ldelim();
                                if (message.metrics != null && message.metrics.length)
                                    for (var i = 0; i < message.metrics.length; ++i)
                                        $root.com.isima.bios.models.proto.Metric.encode(message.metrics[i], writer.uint32(/* id 12, wireType 2 =*/98).fork()).ldelim();
                                if (message.from != null && Object.hasOwnProperty.call(message, "from"))
                                    writer.uint32(/* id 15, wireType 2 =*/122).string(message.from);
                                if (message.where != null && Object.hasOwnProperty.call(message, "where"))
                                    writer.uint32(/* id 20, wireType 2 =*/162).string(message.where);
                                if (message.groupBy != null && Object.hasOwnProperty.call(message, "groupBy"))
                                    $root.com.isima.bios.models.proto.Dimensions.encode(message.groupBy, writer.uint32(/* id 30, wireType 2 =*/242).fork()).ldelim();
                                if (message.windows != null && message.windows.length)
                                    for (var i = 0; i < message.windows.length; ++i)
                                        $root.com.isima.bios.models.proto.Window.encode(message.windows[i], writer.uint32(/* id 40, wireType 2 =*/322).fork()).ldelim();
                                if (message.orderBy != null && Object.hasOwnProperty.call(message, "orderBy"))
                                    $root.com.isima.bios.models.proto.OrderBy.encode(message.orderBy, writer.uint32(/* id 50, wireType 2 =*/402).fork()).ldelim();
                                if (message.limit != null && Object.hasOwnProperty.call(message, "limit"))
                                    writer.uint32(/* id 60, wireType 0 =*/480).int32(message.limit);
                                if (message.onTheFly != null && Object.hasOwnProperty.call(message, "onTheFly"))
                                    writer.uint32(/* id 70, wireType 0 =*/560).bool(message.onTheFly);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified SelectQuery message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.SelectQuery.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectQuery} message SelectQuery message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectQuery.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a SelectQuery message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.SelectQuery} SelectQuery
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectQuery.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.SelectQuery();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.startTime = reader.int64();
                                        break;
                                    case 2:
                                        message.endTime = reader.int64();
                                        break;
                                    case 10:
                                        message.distinct = reader.bool();
                                        break;
                                    case 11:
                                        message.attributes = $root.com.isima.bios.models.proto.AttributeList.decode(reader, reader.uint32());
                                        break;
                                    case 12:
                                        if (!(message.metrics && message.metrics.length))
                                            message.metrics = [];
                                        message.metrics.push($root.com.isima.bios.models.proto.Metric.decode(reader, reader.uint32()));
                                        break;
                                    case 15:
                                        message.from = reader.string();
                                        break;
                                    case 20:
                                        message.where = reader.string();
                                        break;
                                    case 30:
                                        message.groupBy = $root.com.isima.bios.models.proto.Dimensions.decode(reader, reader.uint32());
                                        break;
                                    case 40:
                                        if (!(message.windows && message.windows.length))
                                            message.windows = [];
                                        message.windows.push($root.com.isima.bios.models.proto.Window.decode(reader, reader.uint32()));
                                        break;
                                    case 50:
                                        message.orderBy = $root.com.isima.bios.models.proto.OrderBy.decode(reader, reader.uint32());
                                        break;
                                    case 60:
                                        message.limit = reader.int32();
                                        break;
                                    case 70:
                                        message.onTheFly = reader.bool();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a SelectQuery message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.SelectQuery} SelectQuery
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectQuery.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a SelectQuery message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            SelectQuery.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.startTime != null && message.hasOwnProperty("startTime"))
                                    if (!$util.isInteger(message.startTime) && !(message.startTime && $util.isInteger(message.startTime.low) && $util.isInteger(message.startTime.high)))
                                        return "startTime: integer|Long expected";
                                if (message.endTime != null && message.hasOwnProperty("endTime"))
                                    if (!$util.isInteger(message.endTime) && !(message.endTime && $util.isInteger(message.endTime.low) && $util.isInteger(message.endTime.high)))
                                        return "endTime: integer|Long expected";
                                if (message.distinct != null && message.hasOwnProperty("distinct"))
                                    if (typeof message.distinct !== "boolean")
                                        return "distinct: boolean expected";
                                if (message.attributes != null && message.hasOwnProperty("attributes")) {
                                    var error = $root.com.isima.bios.models.proto.AttributeList.verify(message.attributes);
                                    if (error)
                                        return "attributes." + error;
                                }
                                if (message.metrics != null && message.hasOwnProperty("metrics")) {
                                    if (!Array.isArray(message.metrics))
                                        return "metrics: array expected";
                                    for (var i = 0; i < message.metrics.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.Metric.verify(message.metrics[i]);
                                        if (error)
                                            return "metrics." + error;
                                    }
                                }
                                if (message.from != null && message.hasOwnProperty("from"))
                                    if (!$util.isString(message.from))
                                        return "from: string expected";
                                if (message.where != null && message.hasOwnProperty("where"))
                                    if (!$util.isString(message.where))
                                        return "where: string expected";
                                if (message.groupBy != null && message.hasOwnProperty("groupBy")) {
                                    var error = $root.com.isima.bios.models.proto.Dimensions.verify(message.groupBy);
                                    if (error)
                                        return "groupBy." + error;
                                }
                                if (message.windows != null && message.hasOwnProperty("windows")) {
                                    if (!Array.isArray(message.windows))
                                        return "windows: array expected";
                                    for (var i = 0; i < message.windows.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.Window.verify(message.windows[i]);
                                        if (error)
                                            return "windows." + error;
                                    }
                                }
                                if (message.orderBy != null && message.hasOwnProperty("orderBy")) {
                                    var error = $root.com.isima.bios.models.proto.OrderBy.verify(message.orderBy);
                                    if (error)
                                        return "orderBy." + error;
                                }
                                if (message.limit != null && message.hasOwnProperty("limit"))
                                    if (!$util.isInteger(message.limit))
                                        return "limit: integer expected";
                                if (message.onTheFly != null && message.hasOwnProperty("onTheFly"))
                                    if (typeof message.onTheFly !== "boolean")
                                        return "onTheFly: boolean expected";
                                return null;
                            };
    
                            /**
                             * Creates a SelectQuery message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.SelectQuery} SelectQuery
                             */
                            SelectQuery.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.SelectQuery)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.SelectQuery();
                                if (object.startTime != null)
                                    if ($util.Long)
                                        (message.startTime = $util.Long.fromValue(object.startTime)).unsigned = false;
                                    else if (typeof object.startTime === "string")
                                        message.startTime = parseInt(object.startTime, 10);
                                    else if (typeof object.startTime === "number")
                                        message.startTime = object.startTime;
                                    else if (typeof object.startTime === "object")
                                        message.startTime = new $util.LongBits(object.startTime.low >>> 0, object.startTime.high >>> 0).toNumber();
                                if (object.endTime != null)
                                    if ($util.Long)
                                        (message.endTime = $util.Long.fromValue(object.endTime)).unsigned = false;
                                    else if (typeof object.endTime === "string")
                                        message.endTime = parseInt(object.endTime, 10);
                                    else if (typeof object.endTime === "number")
                                        message.endTime = object.endTime;
                                    else if (typeof object.endTime === "object")
                                        message.endTime = new $util.LongBits(object.endTime.low >>> 0, object.endTime.high >>> 0).toNumber();
                                if (object.distinct != null)
                                    message.distinct = Boolean(object.distinct);
                                if (object.attributes != null) {
                                    if (typeof object.attributes !== "object")
                                        throw TypeError(".com.isima.bios.models.proto.SelectQuery.attributes: object expected");
                                    message.attributes = $root.com.isima.bios.models.proto.AttributeList.fromObject(object.attributes);
                                }
                                if (object.metrics) {
                                    if (!Array.isArray(object.metrics))
                                        throw TypeError(".com.isima.bios.models.proto.SelectQuery.metrics: array expected");
                                    message.metrics = [];
                                    for (var i = 0; i < object.metrics.length; ++i) {
                                        if (typeof object.metrics[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.SelectQuery.metrics: object expected");
                                        message.metrics[i] = $root.com.isima.bios.models.proto.Metric.fromObject(object.metrics[i]);
                                    }
                                }
                                if (object.from != null)
                                    message.from = String(object.from);
                                if (object.where != null)
                                    message.where = String(object.where);
                                if (object.groupBy != null) {
                                    if (typeof object.groupBy !== "object")
                                        throw TypeError(".com.isima.bios.models.proto.SelectQuery.groupBy: object expected");
                                    message.groupBy = $root.com.isima.bios.models.proto.Dimensions.fromObject(object.groupBy);
                                }
                                if (object.windows) {
                                    if (!Array.isArray(object.windows))
                                        throw TypeError(".com.isima.bios.models.proto.SelectQuery.windows: array expected");
                                    message.windows = [];
                                    for (var i = 0; i < object.windows.length; ++i) {
                                        if (typeof object.windows[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.SelectQuery.windows: object expected");
                                        message.windows[i] = $root.com.isima.bios.models.proto.Window.fromObject(object.windows[i]);
                                    }
                                }
                                if (object.orderBy != null) {
                                    if (typeof object.orderBy !== "object")
                                        throw TypeError(".com.isima.bios.models.proto.SelectQuery.orderBy: object expected");
                                    message.orderBy = $root.com.isima.bios.models.proto.OrderBy.fromObject(object.orderBy);
                                }
                                if (object.limit != null)
                                    message.limit = object.limit | 0;
                                if (object.onTheFly != null)
                                    message.onTheFly = Boolean(object.onTheFly);
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a SelectQuery message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @static
                             * @param {com.isima.bios.models.proto.SelectQuery} message SelectQuery
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            SelectQuery.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults) {
                                    object.metrics = [];
                                    object.windows = [];
                                }
                                if (options.defaults) {
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.startTime = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.startTime = options.longs === String ? "0" : 0;
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.endTime = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.endTime = options.longs === String ? "0" : 0;
                                    object.distinct = false;
                                    object.attributes = null;
                                    object.from = "";
                                    object.where = "";
                                    object.groupBy = null;
                                    object.orderBy = null;
                                    object.limit = 0;
                                    object.onTheFly = false;
                                }
                                if (message.startTime != null && message.hasOwnProperty("startTime"))
                                    if (typeof message.startTime === "number")
                                        object.startTime = options.longs === String ? String(message.startTime) : message.startTime;
                                    else
                                        object.startTime = options.longs === String ? $util.Long.prototype.toString.call(message.startTime) : options.longs === Number ? new $util.LongBits(message.startTime.low >>> 0, message.startTime.high >>> 0).toNumber() : message.startTime;
                                if (message.endTime != null && message.hasOwnProperty("endTime"))
                                    if (typeof message.endTime === "number")
                                        object.endTime = options.longs === String ? String(message.endTime) : message.endTime;
                                    else
                                        object.endTime = options.longs === String ? $util.Long.prototype.toString.call(message.endTime) : options.longs === Number ? new $util.LongBits(message.endTime.low >>> 0, message.endTime.high >>> 0).toNumber() : message.endTime;
                                if (message.distinct != null && message.hasOwnProperty("distinct"))
                                    object.distinct = message.distinct;
                                if (message.attributes != null && message.hasOwnProperty("attributes"))
                                    object.attributes = $root.com.isima.bios.models.proto.AttributeList.toObject(message.attributes, options);
                                if (message.metrics && message.metrics.length) {
                                    object.metrics = [];
                                    for (var j = 0; j < message.metrics.length; ++j)
                                        object.metrics[j] = $root.com.isima.bios.models.proto.Metric.toObject(message.metrics[j], options);
                                }
                                if (message.from != null && message.hasOwnProperty("from"))
                                    object.from = message.from;
                                if (message.where != null && message.hasOwnProperty("where"))
                                    object.where = message.where;
                                if (message.groupBy != null && message.hasOwnProperty("groupBy"))
                                    object.groupBy = $root.com.isima.bios.models.proto.Dimensions.toObject(message.groupBy, options);
                                if (message.windows && message.windows.length) {
                                    object.windows = [];
                                    for (var j = 0; j < message.windows.length; ++j)
                                        object.windows[j] = $root.com.isima.bios.models.proto.Window.toObject(message.windows[j], options);
                                }
                                if (message.orderBy != null && message.hasOwnProperty("orderBy"))
                                    object.orderBy = $root.com.isima.bios.models.proto.OrderBy.toObject(message.orderBy, options);
                                if (message.limit != null && message.hasOwnProperty("limit"))
                                    object.limit = message.limit;
                                if (message.onTheFly != null && message.hasOwnProperty("onTheFly"))
                                    object.onTheFly = message.onTheFly;
                                return object;
                            };
    
                            /**
                             * Converts this SelectQuery to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.SelectQuery
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            SelectQuery.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return SelectQuery;
                        })();
    
                        proto.SelectRequest = (function() {
    
                            /**
                             * Properties of a SelectRequest.
                             * @memberof com.isima.bios.models.proto
                             * @interface ISelectRequest
                             * @property {Array.<com.isima.bios.models.proto.ISelectQuery>|null} [queries] SelectRequest queries
                             */
    
                            /**
                             * Constructs a new SelectRequest.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a SelectRequest.
                             * @implements ISelectRequest
                             * @constructor
                             * @param {com.isima.bios.models.proto.ISelectRequest=} [properties] Properties to set
                             */
                            function SelectRequest(properties) {
                                this.queries = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * SelectRequest queries.
                             * @member {Array.<com.isima.bios.models.proto.ISelectQuery>} queries
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @instance
                             */
                            SelectRequest.prototype.queries = $util.emptyArray;
    
                            /**
                             * Creates a new SelectRequest instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectRequest=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.SelectRequest} SelectRequest instance
                             */
                            SelectRequest.create = function create(properties) {
                                return new SelectRequest(properties);
                            };
    
                            /**
                             * Encodes the specified SelectRequest message. Does not implicitly {@link com.isima.bios.models.proto.SelectRequest.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectRequest} message SelectRequest message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectRequest.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.queries != null && message.queries.length)
                                    for (var i = 0; i < message.queries.length; ++i)
                                        $root.com.isima.bios.models.proto.SelectQuery.encode(message.queries[i], writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
                                return writer;
                            };
    
                            /**
                             * Encodes the specified SelectRequest message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.SelectRequest.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectRequest} message SelectRequest message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectRequest.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a SelectRequest message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.SelectRequest} SelectRequest
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectRequest.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.SelectRequest();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        if (!(message.queries && message.queries.length))
                                            message.queries = [];
                                        message.queries.push($root.com.isima.bios.models.proto.SelectQuery.decode(reader, reader.uint32()));
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a SelectRequest message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.SelectRequest} SelectRequest
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectRequest.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a SelectRequest message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            SelectRequest.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.queries != null && message.hasOwnProperty("queries")) {
                                    if (!Array.isArray(message.queries))
                                        return "queries: array expected";
                                    for (var i = 0; i < message.queries.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.SelectQuery.verify(message.queries[i]);
                                        if (error)
                                            return "queries." + error;
                                    }
                                }
                                return null;
                            };
    
                            /**
                             * Creates a SelectRequest message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.SelectRequest} SelectRequest
                             */
                            SelectRequest.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.SelectRequest)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.SelectRequest();
                                if (object.queries) {
                                    if (!Array.isArray(object.queries))
                                        throw TypeError(".com.isima.bios.models.proto.SelectRequest.queries: array expected");
                                    message.queries = [];
                                    for (var i = 0; i < object.queries.length; ++i) {
                                        if (typeof object.queries[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.SelectRequest.queries: object expected");
                                        message.queries[i] = $root.com.isima.bios.models.proto.SelectQuery.fromObject(object.queries[i]);
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a SelectRequest message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @static
                             * @param {com.isima.bios.models.proto.SelectRequest} message SelectRequest
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            SelectRequest.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.queries = [];
                                if (message.queries && message.queries.length) {
                                    object.queries = [];
                                    for (var j = 0; j < message.queries.length; ++j)
                                        object.queries[j] = $root.com.isima.bios.models.proto.SelectQuery.toObject(message.queries[j], options);
                                }
                                return object;
                            };
    
                            /**
                             * Converts this SelectRequest to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.SelectRequest
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            SelectRequest.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return SelectRequest;
                        })();
    
                        proto.QueryResult = (function() {
    
                            /**
                             * Properties of a QueryResult.
                             * @memberof com.isima.bios.models.proto
                             * @interface IQueryResult
                             * @property {number|Long|null} [windowBeginTime] QueryResult windowBeginTime
                             * @property {Array.<com.isima.bios.models.proto.IRecord>|null} [records] QueryResult records
                             */
    
                            /**
                             * Constructs a new QueryResult.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a QueryResult.
                             * @implements IQueryResult
                             * @constructor
                             * @param {com.isima.bios.models.proto.IQueryResult=} [properties] Properties to set
                             */
                            function QueryResult(properties) {
                                this.records = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * QueryResult windowBeginTime.
                             * @member {number|Long} windowBeginTime
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @instance
                             */
                            QueryResult.prototype.windowBeginTime = $util.Long ? $util.Long.fromBits(0,0,false) : 0;
    
                            /**
                             * QueryResult records.
                             * @member {Array.<com.isima.bios.models.proto.IRecord>} records
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @instance
                             */
                            QueryResult.prototype.records = $util.emptyArray;
    
                            /**
                             * Creates a new QueryResult instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {com.isima.bios.models.proto.IQueryResult=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.QueryResult} QueryResult instance
                             */
                            QueryResult.create = function create(properties) {
                                return new QueryResult(properties);
                            };
    
                            /**
                             * Encodes the specified QueryResult message. Does not implicitly {@link com.isima.bios.models.proto.QueryResult.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {com.isima.bios.models.proto.IQueryResult} message QueryResult message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            QueryResult.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.windowBeginTime != null && Object.hasOwnProperty.call(message, "windowBeginTime"))
                                    writer.uint32(/* id 1, wireType 0 =*/8).int64(message.windowBeginTime);
                                if (message.records != null && message.records.length)
                                    for (var i = 0; i < message.records.length; ++i)
                                        $root.com.isima.bios.models.proto.Record.encode(message.records[i], writer.uint32(/* id 10, wireType 2 =*/82).fork()).ldelim();
                                return writer;
                            };
    
                            /**
                             * Encodes the specified QueryResult message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.QueryResult.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {com.isima.bios.models.proto.IQueryResult} message QueryResult message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            QueryResult.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a QueryResult message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.QueryResult} QueryResult
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            QueryResult.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.QueryResult();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        message.windowBeginTime = reader.int64();
                                        break;
                                    case 10:
                                        if (!(message.records && message.records.length))
                                            message.records = [];
                                        message.records.push($root.com.isima.bios.models.proto.Record.decode(reader, reader.uint32()));
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a QueryResult message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.QueryResult} QueryResult
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            QueryResult.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a QueryResult message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            QueryResult.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.windowBeginTime != null && message.hasOwnProperty("windowBeginTime"))
                                    if (!$util.isInteger(message.windowBeginTime) && !(message.windowBeginTime && $util.isInteger(message.windowBeginTime.low) && $util.isInteger(message.windowBeginTime.high)))
                                        return "windowBeginTime: integer|Long expected";
                                if (message.records != null && message.hasOwnProperty("records")) {
                                    if (!Array.isArray(message.records))
                                        return "records: array expected";
                                    for (var i = 0; i < message.records.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.Record.verify(message.records[i]);
                                        if (error)
                                            return "records." + error;
                                    }
                                }
                                return null;
                            };
    
                            /**
                             * Creates a QueryResult message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.QueryResult} QueryResult
                             */
                            QueryResult.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.QueryResult)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.QueryResult();
                                if (object.windowBeginTime != null)
                                    if ($util.Long)
                                        (message.windowBeginTime = $util.Long.fromValue(object.windowBeginTime)).unsigned = false;
                                    else if (typeof object.windowBeginTime === "string")
                                        message.windowBeginTime = parseInt(object.windowBeginTime, 10);
                                    else if (typeof object.windowBeginTime === "number")
                                        message.windowBeginTime = object.windowBeginTime;
                                    else if (typeof object.windowBeginTime === "object")
                                        message.windowBeginTime = new $util.LongBits(object.windowBeginTime.low >>> 0, object.windowBeginTime.high >>> 0).toNumber();
                                if (object.records) {
                                    if (!Array.isArray(object.records))
                                        throw TypeError(".com.isima.bios.models.proto.QueryResult.records: array expected");
                                    message.records = [];
                                    for (var i = 0; i < object.records.length; ++i) {
                                        if (typeof object.records[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.QueryResult.records: object expected");
                                        message.records[i] = $root.com.isima.bios.models.proto.Record.fromObject(object.records[i]);
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a QueryResult message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @static
                             * @param {com.isima.bios.models.proto.QueryResult} message QueryResult
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            QueryResult.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.records = [];
                                if (options.defaults)
                                    if ($util.Long) {
                                        var long = new $util.Long(0, 0, false);
                                        object.windowBeginTime = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                                    } else
                                        object.windowBeginTime = options.longs === String ? "0" : 0;
                                if (message.windowBeginTime != null && message.hasOwnProperty("windowBeginTime"))
                                    if (typeof message.windowBeginTime === "number")
                                        object.windowBeginTime = options.longs === String ? String(message.windowBeginTime) : message.windowBeginTime;
                                    else
                                        object.windowBeginTime = options.longs === String ? $util.Long.prototype.toString.call(message.windowBeginTime) : options.longs === Number ? new $util.LongBits(message.windowBeginTime.low >>> 0, message.windowBeginTime.high >>> 0).toNumber() : message.windowBeginTime;
                                if (message.records && message.records.length) {
                                    object.records = [];
                                    for (var j = 0; j < message.records.length; ++j)
                                        object.records[j] = $root.com.isima.bios.models.proto.Record.toObject(message.records[j], options);
                                }
                                return object;
                            };
    
                            /**
                             * Converts this QueryResult to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.QueryResult
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            QueryResult.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return QueryResult;
                        })();
    
                        proto.SelectQueryResponse = (function() {
    
                            /**
                             * Properties of a SelectQueryResponse.
                             * @memberof com.isima.bios.models.proto
                             * @interface ISelectQueryResponse
                             * @property {Array.<com.isima.bios.models.proto.IQueryResult>|null} [data] SelectQueryResponse data
                             * @property {Array.<com.isima.bios.models.proto.IColumnDefinition>|null} [definitions] SelectQueryResponse definitions
                             * @property {boolean|null} [isWindowedResponse] SelectQueryResponse isWindowedResponse
                             * @property {number|null} [requestQueryNum] SelectQueryResponse requestQueryNum
                             */
    
                            /**
                             * Constructs a new SelectQueryResponse.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a SelectQueryResponse.
                             * @implements ISelectQueryResponse
                             * @constructor
                             * @param {com.isima.bios.models.proto.ISelectQueryResponse=} [properties] Properties to set
                             */
                            function SelectQueryResponse(properties) {
                                this.data = [];
                                this.definitions = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * SelectQueryResponse data.
                             * @member {Array.<com.isima.bios.models.proto.IQueryResult>} data
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @instance
                             */
                            SelectQueryResponse.prototype.data = $util.emptyArray;
    
                            /**
                             * SelectQueryResponse definitions.
                             * @member {Array.<com.isima.bios.models.proto.IColumnDefinition>} definitions
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @instance
                             */
                            SelectQueryResponse.prototype.definitions = $util.emptyArray;
    
                            /**
                             * SelectQueryResponse isWindowedResponse.
                             * @member {boolean} isWindowedResponse
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @instance
                             */
                            SelectQueryResponse.prototype.isWindowedResponse = false;
    
                            /**
                             * SelectQueryResponse requestQueryNum.
                             * @member {number} requestQueryNum
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @instance
                             */
                            SelectQueryResponse.prototype.requestQueryNum = 0;
    
                            /**
                             * Creates a new SelectQueryResponse instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectQueryResponse=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.SelectQueryResponse} SelectQueryResponse instance
                             */
                            SelectQueryResponse.create = function create(properties) {
                                return new SelectQueryResponse(properties);
                            };
    
                            /**
                             * Encodes the specified SelectQueryResponse message. Does not implicitly {@link com.isima.bios.models.proto.SelectQueryResponse.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectQueryResponse} message SelectQueryResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectQueryResponse.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.data != null && message.data.length)
                                    for (var i = 0; i < message.data.length; ++i)
                                        $root.com.isima.bios.models.proto.QueryResult.encode(message.data[i], writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
                                if (message.definitions != null && message.definitions.length)
                                    for (var i = 0; i < message.definitions.length; ++i)
                                        $root.com.isima.bios.models.proto.ColumnDefinition.encode(message.definitions[i], writer.uint32(/* id 2, wireType 2 =*/18).fork()).ldelim();
                                if (message.isWindowedResponse != null && Object.hasOwnProperty.call(message, "isWindowedResponse"))
                                    writer.uint32(/* id 3, wireType 0 =*/24).bool(message.isWindowedResponse);
                                if (message.requestQueryNum != null && Object.hasOwnProperty.call(message, "requestQueryNum"))
                                    writer.uint32(/* id 4, wireType 0 =*/32).int32(message.requestQueryNum);
                                return writer;
                            };
    
                            /**
                             * Encodes the specified SelectQueryResponse message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.SelectQueryResponse.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectQueryResponse} message SelectQueryResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectQueryResponse.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a SelectQueryResponse message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.SelectQueryResponse} SelectQueryResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectQueryResponse.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.SelectQueryResponse();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        if (!(message.data && message.data.length))
                                            message.data = [];
                                        message.data.push($root.com.isima.bios.models.proto.QueryResult.decode(reader, reader.uint32()));
                                        break;
                                    case 2:
                                        if (!(message.definitions && message.definitions.length))
                                            message.definitions = [];
                                        message.definitions.push($root.com.isima.bios.models.proto.ColumnDefinition.decode(reader, reader.uint32()));
                                        break;
                                    case 3:
                                        message.isWindowedResponse = reader.bool();
                                        break;
                                    case 4:
                                        message.requestQueryNum = reader.int32();
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a SelectQueryResponse message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.SelectQueryResponse} SelectQueryResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectQueryResponse.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a SelectQueryResponse message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            SelectQueryResponse.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.data != null && message.hasOwnProperty("data")) {
                                    if (!Array.isArray(message.data))
                                        return "data: array expected";
                                    for (var i = 0; i < message.data.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.QueryResult.verify(message.data[i]);
                                        if (error)
                                            return "data." + error;
                                    }
                                }
                                if (message.definitions != null && message.hasOwnProperty("definitions")) {
                                    if (!Array.isArray(message.definitions))
                                        return "definitions: array expected";
                                    for (var i = 0; i < message.definitions.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.ColumnDefinition.verify(message.definitions[i]);
                                        if (error)
                                            return "definitions." + error;
                                    }
                                }
                                if (message.isWindowedResponse != null && message.hasOwnProperty("isWindowedResponse"))
                                    if (typeof message.isWindowedResponse !== "boolean")
                                        return "isWindowedResponse: boolean expected";
                                if (message.requestQueryNum != null && message.hasOwnProperty("requestQueryNum"))
                                    if (!$util.isInteger(message.requestQueryNum))
                                        return "requestQueryNum: integer expected";
                                return null;
                            };
    
                            /**
                             * Creates a SelectQueryResponse message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.SelectQueryResponse} SelectQueryResponse
                             */
                            SelectQueryResponse.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.SelectQueryResponse)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.SelectQueryResponse();
                                if (object.data) {
                                    if (!Array.isArray(object.data))
                                        throw TypeError(".com.isima.bios.models.proto.SelectQueryResponse.data: array expected");
                                    message.data = [];
                                    for (var i = 0; i < object.data.length; ++i) {
                                        if (typeof object.data[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.SelectQueryResponse.data: object expected");
                                        message.data[i] = $root.com.isima.bios.models.proto.QueryResult.fromObject(object.data[i]);
                                    }
                                }
                                if (object.definitions) {
                                    if (!Array.isArray(object.definitions))
                                        throw TypeError(".com.isima.bios.models.proto.SelectQueryResponse.definitions: array expected");
                                    message.definitions = [];
                                    for (var i = 0; i < object.definitions.length; ++i) {
                                        if (typeof object.definitions[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.SelectQueryResponse.definitions: object expected");
                                        message.definitions[i] = $root.com.isima.bios.models.proto.ColumnDefinition.fromObject(object.definitions[i]);
                                    }
                                }
                                if (object.isWindowedResponse != null)
                                    message.isWindowedResponse = Boolean(object.isWindowedResponse);
                                if (object.requestQueryNum != null)
                                    message.requestQueryNum = object.requestQueryNum | 0;
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a SelectQueryResponse message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @static
                             * @param {com.isima.bios.models.proto.SelectQueryResponse} message SelectQueryResponse
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            SelectQueryResponse.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults) {
                                    object.data = [];
                                    object.definitions = [];
                                }
                                if (options.defaults) {
                                    object.isWindowedResponse = false;
                                    object.requestQueryNum = 0;
                                }
                                if (message.data && message.data.length) {
                                    object.data = [];
                                    for (var j = 0; j < message.data.length; ++j)
                                        object.data[j] = $root.com.isima.bios.models.proto.QueryResult.toObject(message.data[j], options);
                                }
                                if (message.definitions && message.definitions.length) {
                                    object.definitions = [];
                                    for (var j = 0; j < message.definitions.length; ++j)
                                        object.definitions[j] = $root.com.isima.bios.models.proto.ColumnDefinition.toObject(message.definitions[j], options);
                                }
                                if (message.isWindowedResponse != null && message.hasOwnProperty("isWindowedResponse"))
                                    object.isWindowedResponse = message.isWindowedResponse;
                                if (message.requestQueryNum != null && message.hasOwnProperty("requestQueryNum"))
                                    object.requestQueryNum = message.requestQueryNum;
                                return object;
                            };
    
                            /**
                             * Converts this SelectQueryResponse to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.SelectQueryResponse
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            SelectQueryResponse.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return SelectQueryResponse;
                        })();
    
                        proto.SelectResponse = (function() {
    
                            /**
                             * Properties of a SelectResponse.
                             * @memberof com.isima.bios.models.proto
                             * @interface ISelectResponse
                             * @property {Array.<com.isima.bios.models.proto.ISelectQueryResponse>|null} [responses] SelectResponse responses
                             */
    
                            /**
                             * Constructs a new SelectResponse.
                             * @memberof com.isima.bios.models.proto
                             * @classdesc Represents a SelectResponse.
                             * @implements ISelectResponse
                             * @constructor
                             * @param {com.isima.bios.models.proto.ISelectResponse=} [properties] Properties to set
                             */
                            function SelectResponse(properties) {
                                this.responses = [];
                                if (properties)
                                    for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                                        if (properties[keys[i]] != null)
                                            this[keys[i]] = properties[keys[i]];
                            }
    
                            /**
                             * SelectResponse responses.
                             * @member {Array.<com.isima.bios.models.proto.ISelectQueryResponse>} responses
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @instance
                             */
                            SelectResponse.prototype.responses = $util.emptyArray;
    
                            /**
                             * Creates a new SelectResponse instance using the specified properties.
                             * @function create
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectResponse=} [properties] Properties to set
                             * @returns {com.isima.bios.models.proto.SelectResponse} SelectResponse instance
                             */
                            SelectResponse.create = function create(properties) {
                                return new SelectResponse(properties);
                            };
    
                            /**
                             * Encodes the specified SelectResponse message. Does not implicitly {@link com.isima.bios.models.proto.SelectResponse.verify|verify} messages.
                             * @function encode
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectResponse} message SelectResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectResponse.encode = function encode(message, writer) {
                                if (!writer)
                                    writer = $Writer.create();
                                if (message.responses != null && message.responses.length)
                                    for (var i = 0; i < message.responses.length; ++i)
                                        $root.com.isima.bios.models.proto.SelectQueryResponse.encode(message.responses[i], writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
                                return writer;
                            };
    
                            /**
                             * Encodes the specified SelectResponse message, length delimited. Does not implicitly {@link com.isima.bios.models.proto.SelectResponse.verify|verify} messages.
                             * @function encodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {com.isima.bios.models.proto.ISelectResponse} message SelectResponse message or plain object to encode
                             * @param {$protobuf.Writer} [writer] Writer to encode to
                             * @returns {$protobuf.Writer} Writer
                             */
                            SelectResponse.encodeDelimited = function encodeDelimited(message, writer) {
                                return this.encode(message, writer).ldelim();
                            };
    
                            /**
                             * Decodes a SelectResponse message from the specified reader or buffer.
                             * @function decode
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @param {number} [length] Message length if known beforehand
                             * @returns {com.isima.bios.models.proto.SelectResponse} SelectResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectResponse.decode = function decode(reader, length) {
                                if (!(reader instanceof $Reader))
                                    reader = $Reader.create(reader);
                                var end = length === undefined ? reader.len : reader.pos + length, message = new $root.com.isima.bios.models.proto.SelectResponse();
                                while (reader.pos < end) {
                                    var tag = reader.uint32();
                                    switch (tag >>> 3) {
                                    case 1:
                                        if (!(message.responses && message.responses.length))
                                            message.responses = [];
                                        message.responses.push($root.com.isima.bios.models.proto.SelectQueryResponse.decode(reader, reader.uint32()));
                                        break;
                                    default:
                                        reader.skipType(tag & 7);
                                        break;
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Decodes a SelectResponse message from the specified reader or buffer, length delimited.
                             * @function decodeDelimited
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
                             * @returns {com.isima.bios.models.proto.SelectResponse} SelectResponse
                             * @throws {Error} If the payload is not a reader or valid buffer
                             * @throws {$protobuf.util.ProtocolError} If required fields are missing
                             */
                            SelectResponse.decodeDelimited = function decodeDelimited(reader) {
                                if (!(reader instanceof $Reader))
                                    reader = new $Reader(reader);
                                return this.decode(reader, reader.uint32());
                            };
    
                            /**
                             * Verifies a SelectResponse message.
                             * @function verify
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {Object.<string,*>} message Plain object to verify
                             * @returns {string|null} `null` if valid, otherwise the reason why it is not
                             */
                            SelectResponse.verify = function verify(message) {
                                if (typeof message !== "object" || message === null)
                                    return "object expected";
                                if (message.responses != null && message.hasOwnProperty("responses")) {
                                    if (!Array.isArray(message.responses))
                                        return "responses: array expected";
                                    for (var i = 0; i < message.responses.length; ++i) {
                                        var error = $root.com.isima.bios.models.proto.SelectQueryResponse.verify(message.responses[i]);
                                        if (error)
                                            return "responses." + error;
                                    }
                                }
                                return null;
                            };
    
                            /**
                             * Creates a SelectResponse message from a plain object. Also converts values to their respective internal types.
                             * @function fromObject
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {Object.<string,*>} object Plain object
                             * @returns {com.isima.bios.models.proto.SelectResponse} SelectResponse
                             */
                            SelectResponse.fromObject = function fromObject(object) {
                                if (object instanceof $root.com.isima.bios.models.proto.SelectResponse)
                                    return object;
                                var message = new $root.com.isima.bios.models.proto.SelectResponse();
                                if (object.responses) {
                                    if (!Array.isArray(object.responses))
                                        throw TypeError(".com.isima.bios.models.proto.SelectResponse.responses: array expected");
                                    message.responses = [];
                                    for (var i = 0; i < object.responses.length; ++i) {
                                        if (typeof object.responses[i] !== "object")
                                            throw TypeError(".com.isima.bios.models.proto.SelectResponse.responses: object expected");
                                        message.responses[i] = $root.com.isima.bios.models.proto.SelectQueryResponse.fromObject(object.responses[i]);
                                    }
                                }
                                return message;
                            };
    
                            /**
                             * Creates a plain object from a SelectResponse message. Also converts values to other types if specified.
                             * @function toObject
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @static
                             * @param {com.isima.bios.models.proto.SelectResponse} message SelectResponse
                             * @param {$protobuf.IConversionOptions} [options] Conversion options
                             * @returns {Object.<string,*>} Plain object
                             */
                            SelectResponse.toObject = function toObject(message, options) {
                                if (!options)
                                    options = {};
                                var object = {};
                                if (options.arrays || options.defaults)
                                    object.responses = [];
                                if (message.responses && message.responses.length) {
                                    object.responses = [];
                                    for (var j = 0; j < message.responses.length; ++j)
                                        object.responses[j] = $root.com.isima.bios.models.proto.SelectQueryResponse.toObject(message.responses[j], options);
                                }
                                return object;
                            };
    
                            /**
                             * Converts this SelectResponse to JSON.
                             * @function toJSON
                             * @memberof com.isima.bios.models.proto.SelectResponse
                             * @instance
                             * @returns {Object.<string,*>} JSON object
                             */
                            SelectResponse.prototype.toJSON = function toJSON() {
                                return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
                            };
    
                            return SelectResponse;
                        })();
    
                        return proto;
                    })();
    
                    return models;
                })();
    
                return bios;
            })();
    
            return isima;
        })();
    
        return com;
    })();

    return $root;
});

"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ProtobufService = void 0;
const common_1 = require("@nestjs/common");
const node_path_1 = require("node:path");
const protobufjs_1 = __importDefault(require("protobufjs"));
let ProtobufService = class ProtobufService {
    messageType;
    async load() {
        const protoPath = (0, node_path_1.join)(process.cwd(), 'event-contracts', 'order-created.proto');
        const root = await protobufjs_1.default.load(protoPath);
        this.messageType = root.lookupType('ecommerce.OrderCreatedEvent');
    }
    encode(data) {
        const message = this.messageType.create(data);
        return this.messageType.encode(message).finish();
    }
    decode(buffer) {
        return this.messageType.decode(buffer);
    }
};
exports.ProtobufService = ProtobufService;
exports.ProtobufService = ProtobufService = __decorate([
    (0, common_1.Injectable)()
], ProtobufService);
//# sourceMappingURL=protobuf.service.js.map
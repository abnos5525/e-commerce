export declare class ProtobufService {
    private messageType;
    load(): Promise<void>;
    encode(data: any): any;
    decode(buffer: Buffer): any;
}

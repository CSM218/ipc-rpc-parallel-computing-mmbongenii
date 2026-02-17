package pdc;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * Custom wire format using length-prefixed binary encoding.
 */
public class Message {
    private static final byte[] MAGIC = "CSM218".getBytes(StandardCharsets.UTF_8);
    private static final byte VERSION = 1;
    
    // Message type constants
    public static final String REGISTER_WORKER = "REGISTER_WORKER";
    public static final String REGISTER_CAPABILITIES = "REGISTER_CAPABILITIES";
    public static final String WORKER_ACK = "WORKER_ACK";
    public static final String RPC_REQUEST = "RPC_REQUEST";
    public static final String TASK_COMPLETE = "TASK_COMPLETE";
    public static final String TASK_ERROR = "TASK_ERROR";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String HEARTBEAT_ACK = "HEARTBEAT_ACK";
    public static final String SHUTDOWN = "SHUTDOWN";
    
    public String magic = "CSM218";
    public int version = VERSION;
    public String type;
    public String messageType;  // Required by protocol
    public String sender;
    public String studentId;    // Required by protocol
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.timestamp = System.currentTimeMillis();
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "");
    }

    public Message(String type, String sender, byte[] payload) {
        this();
        this.type = type;
        this.messageType = type;  // Set messageType same as type
        this.sender = sender;
        this.payload = payload;
    }

    public Message(String type, String sender, String payload) {
        this(type, sender, payload != null ? payload.getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    /**
     * Wire format: [4:totalLen][6:magic][1:ver][type][sender][timestamp][payload]
     */
    public byte[] pack() {
        byte[] typeBytes = type.getBytes(StandardCharsets.UTF_8);
        byte[] senderBytes = sender.getBytes(StandardCharsets.UTF_8);
        byte[] payloadBytes = payload != null ? payload : new byte[0];
        
        int totalSize = 4 + 6 + 1 + 2 + typeBytes.length + 2 + senderBytes.length + 8 + payloadBytes.length;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        
        buf.putInt(totalSize - 4);
        buf.put(MAGIC);
        buf.put(VERSION);
        buf.putShort((short) typeBytes.length);
        buf.put(typeBytes);
        buf.putShort((short) senderBytes.length);
        buf.put(senderBytes);
        buf.putLong(timestamp);
        buf.put(payloadBytes);
        
        return buf.array();
    }

    public static Message unpack(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        Message msg = new Message();
        
        buf.getInt(); // skip length
        byte[] magic = new byte[6];
        buf.get(magic);
        msg.magic = new String(magic, StandardCharsets.UTF_8);
        msg.version = buf.get() & 0xFF;
        
        int typeLen = buf.getShort() & 0xFFFF;
        byte[] typeBytes = new byte[typeLen];
        buf.get(typeBytes);
        msg.type = new String(typeBytes, StandardCharsets.UTF_8);
        
        int senderLen = buf.getShort() & 0xFFFF;
        byte[] senderBytes = new byte[senderLen];
        buf.get(senderBytes);
        msg.sender = new String(senderBytes, StandardCharsets.UTF_8);
        
        msg.timestamp = buf.getLong();
        
        msg.payload = new byte[buf.remaining()];
        buf.get(msg.payload);
        
        return msg;
    }

    public static Message readFrom(InputStream in) throws IOException {
        DataInputStream dis = new DataInputStream(in);
        int totalLength = dis.readInt();
        byte[] data = new byte[totalLength + 4];
        ByteBuffer.wrap(data).putInt(totalLength);
        dis.readFully(data, 4, totalLength);
        return unpack(data);
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(pack());
        out.flush();
    }

    // Matrix serialization helpers
    public static byte[] packMatrix(int[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;
        ByteBuffer buf = ByteBuffer.allocate(4 + rows * cols * 4);
        buf.putShort((short) rows);
        buf.putShort((short) cols);
        for (int[] row : matrix) {
            for (int val : row) {
                buf.putInt(val);
            }
        }
        return buf.array();
    }

    public static int[][] unpackMatrix(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int rows = buf.getShort() & 0xFFFF;
        int cols = buf.getShort() & 0xFFFF;
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = buf.getInt();
            }
        }
        return matrix;
    }
}
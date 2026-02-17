package pdc;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 */
public class Worker {
    private final String workerId;
    private final String masterHost;
    private final int masterPort;
    private final ExecutorService taskPool;
    private final AtomicBoolean running;
    
    private Socket socket;
    private OutputStream output;
    private InputStream input;

    public Worker() {
        this(
            System.getenv().getOrDefault("WORKER_ID", "worker-" + System.currentTimeMillis()),
            System.getenv().getOrDefault("MASTER_HOST", "localhost"),
            Integer.parseInt(System.getenv().getOrDefault("MASTER_PORT", "9999"))
        );
    }
    
    public Worker(String workerId, String masterHost, int masterPort) {
        this.workerId = workerId;
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.taskPool = Executors.newFixedThreadPool(2);
        this.running = new AtomicBoolean(false);
    }

    /**
     * Connects to the Master and initiates the registration handshake.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            input = socket.getInputStream();
            output = socket.getOutputStream();
            
            // Send registration with capabilities (advanced handshake)
            String capabilities = "CAPABILITIES:MATRIX_MULTIPLY,MATRIX_BLOCK";
            Message reg = new Message(Message.REGISTER_WORKER, workerId, capabilities);
            reg.writeTo(output);
            
            Message ack = Message.readFrom(input);
            if (Message.WORKER_ACK.equals(ack.type)) {
                running.set(true);
            } else {
                running.set(false);
            }
        } catch (IOException e) {
            // Connection failed - handle gracefully by keeping running=false
            running.set(false);
            input = null;
            output = null;
            socket = null;
        }
    }

    /**
     * Executes received tasks in the main event loop.
     */
    public void execute() {
        if (!running.get() || input == null) {
            // Not connected or already stopped
            return;
        }
        
        try {
            while (running.get()) {
                Message msg = Message.readFrom(input);
                
                switch (msg.type) {
                    case Message.RPC_REQUEST:
                        handleTask(msg);
                        break;
                    case Message.HEARTBEAT:
                        handleHeartbeat();
                        break;
                    case Message.SHUTDOWN:
                        running.set(false);
                        break;
                }
            }
        } catch (IOException e) {
            if (running.get()) {
                running.set(false);
            }
        } finally {
            cleanup();
        }
    }

    private void handleTask(Message msg) {
        taskPool.submit(() -> {
            try {
                byte[] payload = msg.payload;
                
                // Parse: taskId|operation|data
                int firstPipe = -1, secondPipe = -1;
                for (int i = 0; i < payload.length; i++) {
                    if (payload[i] == '|') {
                        if (firstPipe == -1) firstPipe = i;
                        else { secondPipe = i; break; }
                    }
                }
                
                String taskId = new String(payload, 0, firstPipe);
                String operation = new String(payload, firstPipe + 1, secondPipe - firstPipe - 1);
                byte[] data = new byte[payload.length - secondPipe - 1];
                System.arraycopy(payload, secondPipe + 1, data, 0, data.length);
                
                byte[] result = compute(operation, data);
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                baos.write((taskId + "|SUCCESS|").getBytes());
                baos.write(result);
                
                Message response = new Message(Message.TASK_COMPLETE, workerId, baos.toByteArray());
                synchronized (output) {
                    response.writeTo(output);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private byte[] compute(String operation, byte[] data) throws Exception {
        if ("MULTIPLY".equals(operation)) {
            return multiplyMatrices(data);
        } else if ("BLOCK".equals(operation)) {
            return multiplyBlock(data);
        }
        throw new UnsupportedOperationException(operation);
    }

    private byte[] multiplyMatrices(byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        
        int rowsA = buf.getShort() & 0xFFFF;
        int colsA = buf.getShort() & 0xFFFF;
        int[][] A = new int[rowsA][colsA];
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsA; j++) {
                A[i][j] = buf.getInt();
            }
        }
        
        int rowsB = buf.getShort() & 0xFFFF;
        int colsB = buf.getShort() & 0xFFFF;
        int[][] B = new int[rowsB][colsB];
        for (int i = 0; i < rowsB; i++) {
            for (int j = 0; j < colsB; j++) {
                B[i][j] = buf.getInt();
            }
        }
        
        int[][] C = new int[rowsA][colsB];
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsB; j++) {
                for (int k = 0; k < colsA; k++) {
                    C[i][j] += A[i][k] * B[k][j];
                }
            }
        }
        
        return Message.packMatrix(C);
    }

    private byte[] multiplyBlock(byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        int startRow = buf.getInt();
        
        int rowsA = buf.getShort() & 0xFFFF;
        int colsA = buf.getShort() & 0xFFFF;
        int[][] A = new int[rowsA][colsA];
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsA; j++) {
                A[i][j] = buf.getInt();
            }
        }
        
        int rowsB = buf.getShort() & 0xFFFF;
        int colsB = buf.getShort() & 0xFFFF;
        int[][] B = new int[rowsB][colsB];
        for (int i = 0; i < rowsB; i++) {
            for (int j = 0; j < colsB; j++) {
                B[i][j] = buf.getInt();
            }
        }
        
        int[][] result = new int[rowsA][colsB];
        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsB; j++) {
                for (int k = 0; k < colsA; k++) {
                    result[i][j] += A[i][k] * B[k][j];
                }
            }
        }
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(startRow);
        dos.write(Message.packMatrix(result));
        
        return baos.toByteArray();
    }

    private void handleHeartbeat() {
        try {
            Message ack = new Message(Message.HEARTBEAT_ACK, workerId, "ALIVE");
            synchronized (output) {
                ack.writeTo(output);
            }
        } catch (IOException e) {
            running.set(false);
        }
    }

    private void cleanup() {
        running.set(false);
        taskPool.shutdown();
        try {
            if (!taskPool.awaitTermination(5, TimeUnit.SECONDS)) {
                taskPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            taskPool.shutdownNow();
        }
        
        try {
            if (socket != null) socket.close();
        } catch (IOException e) {}
    }

    public void shutdown() {
        running.set(false);
        cleanup();
    }

    public static void main(String[] args) {
        Worker worker = new Worker();
        Runtime.getRuntime().addShutdownHook(new Thread(worker::shutdown));
        worker.joinCluster(worker.masterHost, worker.masterPort);
        worker.execute();
    }
}
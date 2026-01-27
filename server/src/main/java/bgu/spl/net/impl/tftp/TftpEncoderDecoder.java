package bgu.spl.net.impl.tftp;

import java.io.ByteArrayOutputStream;

import bgu.spl.net.api.MessageEncoderDecoder;

public class TftpEncoderDecoder implements MessageEncoderDecoder<byte[]> {
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    private int opcode = -1;
    private int expectedLength = -1;

    @Override
    public byte[] decodeNextByte(byte nextByte) {
        buffer.write(nextByte);

        if (opcode == -1 && buffer.size() == 2) {
            byte[] data = buffer.toByteArray();
            opcode = ((data[0] & 0xFF) << 8) | (data[1] & 0xFF);
            if (opcode == 6 || opcode == 10) {
                expectedLength = 2;
            } else if (opcode == 4) {
                expectedLength = 4;
            }
        }

        if (opcode == 3 && expectedLength == -1 && buffer.size() == 4) {
            byte[] data = buffer.toByteArray();
            int packetSize = ((data[2] & 0xFF) << 8) | (data[3] & 0xFF);
            expectedLength = 6 + packetSize;
        }

        if (expectedLength != -1 && buffer.size() == expectedLength) {
            return popBuffer();
        }

        if (opcode == 1 || opcode == 2 || opcode == 7 || opcode == 8) {
            if (nextByte == 0 && buffer.size() >= 3) {
                return popBuffer();
            }
        } else if (opcode == 5) {
            if (nextByte == 0 && buffer.size() >= 5) {
                return popBuffer();
            }
        } else if (opcode == 9) {
            if (nextByte == 0 && buffer.size() >= 4) {
                return popBuffer();
            }
        } else if (opcode != -1 && expectedLength == -1) {
            if (!isKnownOpcode(opcode)) {
                return popBuffer();
            }
        }

        return null;
    }

    @Override
    public byte[] encode(byte[] message) {
        return message;
    }

    private byte[] popBuffer() {
        byte[] result = buffer.toByteArray();
        buffer.reset();
        opcode = -1;
        expectedLength = -1;
        return result;
    }

    private boolean isKnownOpcode(int code) {
        return code == 1 || code == 2 || code == 3 || code == 4 || code == 5
                || code == 6 || code == 7 || code == 8 || code == 9 || code == 10;
    }
}

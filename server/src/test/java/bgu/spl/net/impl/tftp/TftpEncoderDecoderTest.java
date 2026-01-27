package bgu.spl.net.impl.tftp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

public class TftpEncoderDecoderTest {

    @Test
    public void decodeAllOpcodes() {
        assertPacketDecoded(buildStringPacket(1, "file.txt"));
        assertPacketDecoded(buildStringPacket(2, "upload.bin"));
        assertPacketDecoded(buildStringPacket(7, "user"));
        assertPacketDecoded(buildStringPacket(8, "delete.me"));
        assertPacketDecoded(buildDirq());
        assertPacketDecoded(buildDisc());
        assertPacketDecoded(buildAck(12));
        assertPacketDecoded(buildError(1, "oops"));
        assertPacketDecoded(buildBcast(true, "new.txt"));
    }

    @Test
    public void decodeDataPacketSizes() {
        assertPacketDecoded(buildData(0, 1));
        assertPacketDecoded(buildData(1, 2));
        assertPacketDecoded(buildData(511, 3));
        assertPacketDecoded(buildData(512, 4));
    }

    private void assertPacketDecoded(byte[] packet) {
        TftpEncoderDecoder encdec = new TftpEncoderDecoder();
        byte[] result = null;
        for (int i = 0; i < packet.length; i++) {
            result = encdec.decodeNextByte(packet[i]);
            if (i < packet.length - 1) {
                assertNull(result);
            }
        }
        assertNotNull(result);
        assertArrayEquals(packet, result);
    }

    private byte[] buildStringPacket(int opcode, String value) {
        byte[] nameBytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] packet = new byte[nameBytes.length + 3];
        packet[0] = 0;
        packet[1] = (byte) opcode;
        System.arraycopy(nameBytes, 0, packet, 2, nameBytes.length);
        packet[packet.length - 1] = 0;
        return packet;
    }

    private byte[] buildDirq() {
        return new byte[] { 0, 6 };
    }

    private byte[] buildDisc() {
        return new byte[] { 0, 10 };
    }

    private byte[] buildAck(int block) {
        return new byte[] { 0, 4, (byte) ((block >> 8) & 0xFF), (byte) (block & 0xFF) };
    }

    private byte[] buildError(int code, String message) {
        byte[] msg = message.getBytes(StandardCharsets.UTF_8);
        byte[] packet = new byte[4 + msg.length + 1];
        packet[0] = 0;
        packet[1] = 5;
        packet[2] = (byte) ((code >> 8) & 0xFF);
        packet[3] = (byte) (code & 0xFF);
        System.arraycopy(msg, 0, packet, 4, msg.length);
        packet[packet.length - 1] = 0;
        return packet;
    }

    private byte[] buildBcast(boolean isAdded, String name) {
        byte[] msg = name.getBytes(StandardCharsets.UTF_8);
        byte[] packet = new byte[3 + msg.length + 1];
        packet[0] = 0;
        packet[1] = 9;
        packet[2] = (byte) (isAdded ? 1 : 0);
        System.arraycopy(msg, 0, packet, 3, msg.length);
        packet[packet.length - 1] = 0;
        return packet;
    }

    private byte[] buildData(int size, int block) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte) (i & 0xFF);
        }
        byte[] packet = new byte[6 + size];
        packet[0] = 0;
        packet[1] = 3;
        packet[2] = (byte) ((size >> 8) & 0xFF);
        packet[3] = (byte) (size & 0xFF);
        packet[4] = (byte) ((block >> 8) & 0xFF);
        packet[5] = (byte) (block & 0xFF);
        System.arraycopy(data, 0, packet, 6, size);
        return packet;
    }
}

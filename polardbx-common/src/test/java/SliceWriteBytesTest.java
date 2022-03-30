import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class SliceWriteBytesTest {

    @Test
    public void writeLongerBytesShouldThrowEx() {
        Slice slice = Slices.allocate(1000);
        SliceOutput sliceOutput = slice.getOutput();
        boolean fail = false;
        try {
            sliceOutput.writeBytes(new byte[1024]);
        } catch (IndexOutOfBoundsException ex) {
            fail = true;
        }
        Assert.assertTrue(fail);
    }

    @Test
    public void wirteLessBytesShouldSuccess() {
        Slice slice = Slices.allocate(1000);
        SliceOutput sliceOutput = slice.getOutput();
        boolean fail = false;
        try {
            sliceOutput.appendInt(1);
            sliceOutput.writeBytes(new byte[16]);
            sliceOutput.writeBytes(new byte[512]);
            sliceOutput.appendByte(0);
        } catch (IndexOutOfBoundsException ex) {
            fail = true;
        }
        Assert.assertFalse(fail);
    }
}

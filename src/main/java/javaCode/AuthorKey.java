package javaCode;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class AuthorKey implements WritableComparable<AuthorKey> {

    Text a1Name;
    Text a2Name;

    public AuthorKey(Text a1Name, Text a2Name) {
        this.a1Name = a1Name;
        this.a2Name = a2Name;

    }

    public AuthorKey() {
        this.a1Name = new Text();
        this.a2Name = new Text();
    }

    @Override
    public int compareTo(AuthorKey o) {
        if (o == null)
            return 0;
        int a1 = o.a1Name.compareTo(o.a1Name);
        int a2 = o.a2Name.compareTo(o.a2Name);
        if (a1 != 0) {
            return a1;
        } else if (a2 != 0) {
            return a2;

        }
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((a1Name == null) ? 0 : a1Name.hashCode());
        result = prime * result + ((a2Name == null) ? 0 : a2Name.hashCode());
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.a1Name.write(dataOutput);
        this.a2Name.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.a1Name.readFields(dataInput);
        this.a2Name.readFields(dataInput);

    }
    @Override
    public String toString() {

        return a1Name.toString() + ":" + a2Name.toString();
    }
}

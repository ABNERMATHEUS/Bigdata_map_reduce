package advanced.entropy;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Auxiliar implements Writable {
    private String base;
    private long qtd;


    public Auxiliar() {
    }

    public Auxiliar(String base, long qtd) {
        this.base = base;
        this.qtd = qtd;
    }

    public String getBase() {
        return base;
    }

    public void setBase(String base) {
        this.base = base;
    }

    public long getQtd() {
        return qtd;
    }

    public void setQtd(long qtd) {
        this.qtd = qtd;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(base);
        dataOutput.writeUTF(String.valueOf(qtd));
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        base = dataInput.readUTF();
        qtd = Long.parseLong(dataInput.readUTF());
    }


}

package org.grifols.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CoderException;

@SuppressWarnings("serial")
public class DeterministicKeyCoderOrcl extends AtomicCoder<orclTable> {

    public static DeterministicKeyCoderOrcl of() {
        return INSTANCE;
    }

    private static final DeterministicKeyCoderOrcl INSTANCE = new DeterministicKeyCoderOrcl();

    private DeterministicKeyCoderOrcl() {
    }

    @Override
    public void encode(orclTable value, OutputStream outStream) throws CoderException, IOException {

        ObjectOutputStream data = new ObjectOutputStream(outStream);

        data.writeObject(value);

    }

    @Override
    public orclTable decode(InputStream inStream) throws CoderException, IOException {

        ObjectInputStream data = new ObjectInputStream(inStream);
        orclTable value = null;
        try {
            value = (orclTable) data.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return value;

    }

    @Override
    public void verifyDeterministic() {
    }

}

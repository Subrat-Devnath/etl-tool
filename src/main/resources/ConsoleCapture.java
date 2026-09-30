package sidecar;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

public final class ConsoleCapture {

    private ConsoleCapture() {
    }

    /**
     * Spark prints through scala.Console, which caches System.out when it is first
     * loaded, so redirecting System.out is not enough to capture show()/printSchema().
     */
    public static String capture(Runnable body) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        PrintStream stream = new PrintStream(buffer, true);

        scala.Console$.MODULE$.withOut(stream, new AbstractFunction0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
                body.run();
                return BoxedUnit.UNIT;
            }
        });

        stream.flush();
        return buffer.toString();
    }

}

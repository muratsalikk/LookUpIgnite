package org.murat;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;

/*
 * Runs always, create task if a new file occurs
 */
public class Stream {

    void startStream() {
        String dirName = "C:/Users/murat.salik/Desktop/landing";
        while(true) {
            try {
                Files.list(new File(dirName).toPath())
                        .limit(10)
                        .forEach(filename -> new Task(filename));
                TimeUnit.SECONDS.sleep(10);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

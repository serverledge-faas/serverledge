package com.test;

// The compiled jar used by serverledge-cli is in ./java_build/target


import java.util.Map;
import java.util.HashMap;


public class HelloFunction {


    public Map<String, Object> handler(Object params, Map<String, Object> context) {
        // Test output capture (stdout)
        System.out.println("Log: Iniziata esecuzione Java Function!");
        System.out.println("Log: Parametri ricevuti: " + params);

        // Test output capture (stderr)
        System.err.println("[DEBUG]: This is a debug message on stderr");


        Map<String, Object> result = new HashMap<>();
        result.put("message", "Hello from the Java default runtime of Serverledge!");
        result.put("received_params", params);
        result.put("java_version", System.getProperty("java.version"));

        return result;
    }
}
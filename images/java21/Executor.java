import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class Executor {

    private static final int PORT = 8080;

    // GSON to parse serverledge's jsons
    private static final Gson gson = new GsonBuilder()
                                        .disableHtmlEscaping()
                                        .serializeNulls()
                                        .create();


    private static volatile URLClassLoader sharedClassLoader = null;
    private static final Object LOADER_LOCK = new Object();

    public static void main(String[] args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);
        server.createContext("/", new InvokeHandler());
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        System.out.println("Java Executor Server started on port " + PORT);
    }

    static class InvokeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            if (!exchange.getRequestMethod().equalsIgnoreCase("POST") || !exchange.getRequestURI().getPath().contains("invoke")) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }

            InputStreamReader reader = new InputStreamReader(exchange.getRequestBody(), StandardCharsets.UTF_8);
            Map<String, Object> request = gson.fromJson(reader, new TypeToken<Map<String, Object>>(){}.getType());

            String handlerStr = (String) request.get("Handler");
            String handlerDir = (String) request.get("HandlerDir");
            Object params = request.get("Params");
            boolean returnOutput = request.containsKey("ReturnOutput") && (boolean) request.get("ReturnOutput");

            String envContext = System.getenv("CONTEXT");
            Map<String, Object> context = envContext != null ?
                gson.fromJson(envContext, new TypeToken<Map<String, Object>>(){}.getType()) : new HashMap<>();

            Map<String, Object> response = new HashMap<>();

            PrintStream originalOut = System.out;
            PrintStream originalErr = System.err;
            ByteArrayOutputStream baosOut = new ByteArrayOutputStream();
            ByteArrayOutputStream baosErr = new ByteArrayOutputStream();


            try {
                if (returnOutput) {
                    System.setOut(new PrintStream(baosOut));
                    System.setErr(new PrintStream(baosErr));
                }

                if (sharedClassLoader == null) {
                    synchronized (LOADER_LOCK) {
                        if (sharedClassLoader == null) {
                            File dir = new File(handlerDir);
                            if (!dir.exists() || !dir.isDirectory()) {
                                throw new FileNotFoundException("Handler Directory not found: " + handlerDir);
                            }
                            // building the classpath
                            List<URL> urls = new ArrayList<>();
                            urls.add(dir.toURI().toURL());

                            File[] jarFiles = dir.listFiles((d, name) -> name.toLowerCase().endsWith(".jar"));
                            if (jarFiles != null) {
                                for (File jar : jarFiles) {
                                    urls.add(jar.toURI().toURL());
                                }
                            }

                            sharedClassLoader = new URLClassLoader(urls.toArray(new URL[0]), Executor.class.getClassLoader());
                            System.out.println("Loader created for: " + handlerDir);
                        }
                    }
                }

                String className;
                String methodName;
                if (handlerStr.contains("::")) {
                    String[] parts = handlerStr.split("::");
                    className = parts[0];
                    methodName = parts[1];
                } else {
                    className = handlerStr;
                    methodName = "handler"; // default name if we're only provided with the class name
                }

                Class<?> usrClass = Class.forName(className, true, sharedClassLoader);
                Object instance = usrClass.getDeclaredConstructor().newInstance();

                Method method = null;
                for (Method m : usrClass.getMethods()) {
                    if (m.getName().equals(methodName)) {
                        method = m;
                        break;
                    }
                }

                if (method == null) throw new NoSuchMethodException("Method " + methodName + " not found in " + className);

                Object result;
                if (method.getParameterCount() == 2) {
                    result = method.invoke(instance, params, context);
                } else {
                    result = method.invoke(instance, params);
                }
                
                response.put("Result", gson.toJson(result));
                response.put("Success", true);

            } catch (Exception e) {
                e.printStackTrace();
                response.put("Success", false);
                response.put("Error", e.toString());
                response.put("Result", "");
            } finally {
                if (returnOutput) {
                    System.out.flush();
                    System.err.flush();
                    System.setOut(originalOut);
                    System.setErr(originalErr);

                    String stdoutCaptured = baosOut.toString(StandardCharsets.UTF_8);
                    String stderrCaptured = baosErr.toString(StandardCharsets.UTF_8);
                    response.put("Output", stdoutCaptured + "\n" + stderrCaptured);
                } else {
                    response.put("Output", "");
                }
            }

            String jsonResponse = gson.toJson(response);
            byte[] responseBytes = jsonResponse.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(responseBytes);
            os.close();
        }
    }
}
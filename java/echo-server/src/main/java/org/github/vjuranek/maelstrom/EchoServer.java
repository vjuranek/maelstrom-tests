package org.github.vjuranek.maelstrom;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class EchoServer {

    private String nodeId;
    private int nextMsgId;

    public EchoServer() {
        this.nextMsgId = 0;
    }

    public void reply(Map<String, Object> req, Map<String, Object> respBody) {
        this.nextMsgId += 1;

        Map<String, Object> resp = new HashMap<>();
        resp.put("src", this.nodeId);
        resp.put("dest", req.get("src"));
        resp.put("body", respBody);

        Gson gson = new GsonBuilder().create();
        String respJson = gson.toJson(resp);

        log("RESP: %s", respJson);
        send(respJson);
    }

    public void run() {
        Scanner in = new Scanner(System.in);
        Gson gson = new Gson();

        while (in.hasNextLine()) {
            String line = in.nextLine();
            log("REQ: %s", line);

            Map<String, Object> req = new HashMap();
            req = (Map<String, Object>) gson.fromJson(line, req.getClass());
            Map<String, Object> reqBody = (Map<String, Object>) req.get("body");

            Map<String, Object> respBody = new HashMap<>();
            respBody.put("msg_id", this.nextMsgId);
            respBody.put("in_reply_to", ((Double) reqBody.get("msg_id")).intValue()); // gson parses msg_id as double

            switch ((String) reqBody.get("type")) {
                case "init":
                    this.nodeId = (String) reqBody.get("node_id");
                    respBody.put("type", "init_ok");
                    this.reply(req, respBody);
                    break;

                case "echo":
                    respBody.put("type", "echo_ok");
                    respBody.put("echo", reqBody.get("echo"));
                    this.reply(req, respBody);
                    break;

                default:
                    throw new IllegalStateException("Unsupported message type " + reqBody.get("type"));
            }
        }
    }

    private static void send(String msg) {
        System.out.println(msg);
    }

    private static void log(String msg, String ... args) {
        System.err.printf(msg, args);
    }

    public static void main(String[] args) {
        EchoServer server = new EchoServer();
        server.run();
    }
}

package org.github.vjuranek.maelstrom;

import java.util.Scanner;

import com.google.gson.Gson;
import org.github.vjuranek.maelstrom.dto.Request;
import org.github.vjuranek.maelstrom.dto.Response;
import org.github.vjuranek.maelstrom.dto.ResponseBody;


public class EchoServer {

    private String nodeId;
    private int nextMsgId;

    public EchoServer() {
        this.nextMsgId = 0;
    }

    public void reply(Response resp) {
        this.nextMsgId += 1;

        String respJson = resp.asJson();
        log("RESP: %s", respJson);
        send(respJson);
    }

    public void run() {
        Scanner in = new Scanner(System.in);
        Gson gson = new Gson();

        while (in.hasNextLine()) {
            String line = in.nextLine();
            log("REQ: %s", line);
            Request req = new Request(gson, line);

            ResponseBody respBody = new ResponseBody(this.nextMsgId);
            respBody.withInReplyTo(req.getBody().getMsgId());

            Response resp = new Response(this.nodeId);
            resp.withDest(req.getSrc());
            resp.withBody(respBody);

            switch (req.getBody().getType()) {
                case "init":
                    this.nodeId = (String) req.getBody().get("node_id");
                    respBody.withType("init_ok");
                    this.reply(resp);
                    break;

                case "echo":
                    respBody.withType("echo_ok");
                    respBody.with("echo", req.getBody().get("echo"));
                    this.reply(resp);
                    break;

                default:
                    throw new IllegalStateException("Unsupported message type " + req.getBody().getType());
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

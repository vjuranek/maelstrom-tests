package org.github.vjuranek.maelstrom.dto;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Response {

    private final Gson gson;
    private final String src;
    private final Map<String, Object> resp;

    private ResponseBody body;

    public Response(String src) {
        this.gson = new GsonBuilder().create();
        this.src = src;
        this.resp = new HashMap<>();
    }

    public Response(Gson gson, String src) {
        this.gson = gson;
        this.src = src;
        this.resp = new HashMap<>();
    }

    public void forRequest(Request req, int msgId) {
        this.body = new ResponseBody(msgId);
        this.body.withInReplyTo(req.getBody().getMsgId());
        this.withBody(this.body);
        this.withDest(req.getSrc());
    }

    public void withDest(String dest) {
        this.resp.put("dest", dest);
    }

    public void withBody(ResponseBody body) {
        this.resp.put("body", body.asMap());
    }

    public ResponseBody getBody() {
        if (this.body == null) {
            throw new IllegalStateException("Cannot access body before providing request");
        }

        return this.body;
    }

    public String asJson() {
        return this.gson.toJson(resp);
    }
}

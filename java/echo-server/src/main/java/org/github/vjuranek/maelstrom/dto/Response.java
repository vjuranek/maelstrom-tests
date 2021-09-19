package org.github.vjuranek.maelstrom.dto;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Response {

    private final Gson gson;
    private final String src;
    private final Map<String, Object> resp;

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

    public void withDest(String dest) {
        this.resp.put("dest", dest);
    }

    public void withBody(ResponseBody body) {
        this.resp.put("body", body.asMap());
    }

    public String asJson() {
        return this.gson.toJson(resp);
    }
}

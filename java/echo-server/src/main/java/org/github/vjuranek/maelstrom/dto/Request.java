package org.github.vjuranek.maelstrom.dto;

import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Request {

    private Gson gson;
    private String reqStr;
    private Map<String, Object> request;
    private RequestBody body;

    public Request(String reqStr) {
        this.reqStr = reqStr;
        this.gson = new GsonBuilder().create();
        this.parseRequest();
    }

    public Request(Gson gson,String reqStr) {
        this.reqStr = reqStr;
        this.gson = gson;
        this.parseRequest();
    }

    private void parseRequest() {
        Map<String, Object> req = new HashMap();
        this.request = (Map<String, Object>) gson.fromJson(this.reqStr, req.getClass());
        this.body = new RequestBody((Map<String, Object>) req.get("body"));
    }

    public RequestBody getBody() {
        return this.body;
    }

    public String getSrc() {
        return (String) this.request.get("src");
    }

}

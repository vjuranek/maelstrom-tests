package org.github.vjuranek.maelstrom.dto;

import java.util.Map;

public class RequestBody {

    private Map<String, Object> body;

    public RequestBody(Map<String, Object> body) {
        this.body = body;
    }

    public Object get(String key) {
        return body.get(key);
    }

    public String getType() {
        return (String) body.get("type");
    }

    public int getMsgId() {
        return ((Double) body.get("msg_id")).intValue(); // gson parses msg_id as double
    }
}

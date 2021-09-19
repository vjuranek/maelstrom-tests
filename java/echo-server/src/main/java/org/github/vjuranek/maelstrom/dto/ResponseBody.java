package org.github.vjuranek.maelstrom.dto;

import java.util.HashMap;
import java.util.Map;

public class ResponseBody {

    private Map<String, Object> body;

    public ResponseBody(int msgId) {
        this.body = new HashMap<>();
        this.body.put("msg_id", msgId);
    }

    public void with(String key, Object value) {
        this.body.put(key, value);
    }

    public void withType(String type) {
        this.body.put("type", type);
    }

    public void withInReplyTo(int inReply) {
        this.body.put("in_reply_to", inReply);
    }

    public Map<String, Object> asMap() {
        return body;
    }
}

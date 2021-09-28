package main

import (
	"bufio"
	"fmt"
	"os"

	"encoding/json"
)


func reply(req map[string]interface{}, body map[string]interface{}, nodeId string, msgId int) {
	resp := map[string]interface{}{
		"src": nodeId,
		"dest": req["src"].(string),
		"body": body,
	}
	reply, _ := json.Marshal(resp)
	fmt.Println(string((reply)))
}

func main() {
	var msgId = 0
	var nodeId string
	
	scanner := bufio.NewScanner(os.Stdin)
	
	for scanner.Scan() {
		var rawReq = scanner.Text()
		var req map[string]interface{}
		_ = json.Unmarshal([]byte(rawReq), &req)
		var body = req["body"].(map[string]interface{})

		msgId = msgId + 1
		respBody := map[string]interface{} {	
			"msg_id": msgId,
			"in_reply_to": int(body["msg_id"].(float64)),
		}
		
		switch reqType := body["type"]; reqType {
		case "init":
			nodeId = body["node_id"].(string)			
			respBody["type"] = "init_ok"
		case "echo":
			respBody["type"] = "echo_ok"
			respBody["echo"] = body["echo"].(string)
		}

		reply(req, respBody, nodeId, msgId)
	}
}

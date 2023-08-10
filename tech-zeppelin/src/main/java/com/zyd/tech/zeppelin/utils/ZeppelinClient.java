package com.zyd.tech.zeppelin.utils;

import kong.unirest.GetRequest;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.apache.ApacheClient;
import kong.unirest.json.JSONArray;
import kong.unirest.json.JSONObject;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.zeppelin.client.ClientConfig;
import org.apache.zeppelin.client.NoteResult;
import org.apache.zeppelin.client.ParagraphResult;
import org.apache.zeppelin.client.ZSession;
import org.apache.zeppelin.common.SessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import unirest.shaded.org.apache.http.client.HttpClient;
import unirest.shaded.org.apache.http.impl.client.HttpClients;

import javax.net.ssl.SSLContext;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: base
 * @description: ZeppelinClient重写
 * @author: zhongyuandong
 * @create: 2023-08-10 17:54:51
 * @Version 1.0
 **/
public class ZeppelinClient extends org.apache.zeppelin.client.ZeppelinClient{

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeppelinClient.class);

    public ZeppelinClient(ClientConfig clientConfig) throws Exception {
        super(clientConfig);
    }

    /*
     * @Description: 删除paragraph
     * @Param: [noteId, paragraphId]
     * @return: void
     * @Author: zhongyuandong
     * @Date: 2023-08-10 17:58
     */
    public void deleteParagraph(String noteId, String paragraphId) throws Exception {
        HttpResponse<JsonNode> response = Unirest
                .delete("/notebook/{noteId}/paragraph/{paragraphId}")
                .routeParam("noteId", noteId)
                .routeParam("paragraphId", paragraphId)
                .asJson();
        checkResponse(response);
        JsonNode jsonNode = response.getBody();
        checkJsonNodeStatus(jsonNode);
    }

    private void checkResponse(HttpResponse<JsonNode> response) throws Exception {
        if (response.getStatus() == 302) {
            throw new Exception("Please login first");
        }
        if (response.getStatus() != 200) {
            String message = response.getStatusText();
            if (response.getBody() != null &&
                    response.getBody().getObject() != null &&
                    response.getBody().getObject().has("message")) {
                message = response.getBody().getObject().getString("message");
            }
            throw new Exception(String.format("Unable to call rest api, status: %s, statusText: %s, message: %s",
                    response.getStatus(),
                    response.getStatusText(),
                    message));
        }
    }

    private void checkJsonNodeStatus(JsonNode jsonNode) throws Exception {
        if (! "OK".equalsIgnoreCase(jsonNode.getObject().getString("status"))) {
            throw new Exception(StringEscapeUtils.unescapeJava(jsonNode.getObject().getString("message")));
        }
    }

}

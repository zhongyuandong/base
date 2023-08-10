package com.zyd.tech.zeppelin.restapi;

import com.alibaba.fastjson.JSON;
import com.zyd.common.utils.OkHttpUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.zeppelin.client.*;
import com.zyd.tech.zeppelin.utils.ZeppelinClient;
import org.apache.zeppelin.common.SessionInfo;

import java.util.List;

/**
 * @program: base
 * @description: Paragraph REST API测试
 * @author: zhongyuandong
 * @create: 2023-08-10 14:42:49
 * @Version 1.0
 **/
public class ParagraphOperations {

    public static void main(String[] args) throws Exception {
//        String userPassword = "bigdata_dev" + ":" + "BS0opayMXf5JRhSg";
//        userPassword = "8fc74cbb-0542-4895-9e78-f101de5cee7d";
//        String encodedAuth = Base64.getEncoder().encodeToString(userPassword.getBytes());
//        System.out.println(encodedAuth);
        deleteParagraph();

//        HttpResponse<JsonNode> response = ((HttpRequestWithBody)((HttpRequestWithBody) Unirest.delete("/notebook/{noteId}/paragraph/{paragraphId}")
//                .routeParam("noteId", "2JAB4GBJ7")).routeParam("paragraphId", "paragraph_1691657389337_1354488910")).asJson();
//        System.out.println(response);
//        clientApi();
    }

    public static void deleteParagraph () throws Exception{
        ClientConfig clientConfig = new ClientConfig("http://uat-bigdata-20-84:8281");
        ZeppelinClient zClient = new ZeppelinClient(clientConfig);
        zClient.login("admin", "BuS0FjTHcgbipFa5");
        String noteId = "2JAB4GBJ7";
        //查询node 信息，sdk ，不存在会抛出异常
        NoteResult renamedNoteResult = zClient.queryNoteResult(noteId);
        List<ParagraphResult> paragraphResultList = renamedNoteResult.getParagraphResultList();
        if (CollectionUtils.isEmpty(paragraphResultList)){
            return;
        }
        paragraphResultList.forEach(graph -> {
            if (Status.ERROR == graph.getStatus()){
                try {
                    String paragraphId = graph.getParagraphId();
                    System.out.println(paragraphId);
                    zClient.deleteParagraph(noteId, paragraphId);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        System.out.println(JSON.toJSONString(paragraphResultList));
        renamedNoteResult = zClient.queryNoteResult(noteId);
        System.out.println(renamedNoteResult);
    }

    public static void getTheStatus (){
        String url = "http://uat-bigdata-20-84:8281/api/notebook/job/2J7VFYDQJ/paragraph_1691576494269_769428519";
        String async = OkHttpUtils.builder().url(url).get().async();
        System.out.println(async);
    }

    public static void clientApi () throws Exception {
        ClientConfig clientConfig = new ClientConfig("http://uat-bigdata-20-84:8281");
        ZeppelinClient zClient = new ZeppelinClient(clientConfig);
        zClient.login("admin", "BuS0FjTHcgbipFa5");


        String zeppelinVersion = zClient.getVersion();
        System.out.println("Zeppelin version: " + zeppelinVersion);

        String notePath = "/zeppelin_client_examples/note_3";
        String noteId = null;
        try {
            // 判断node是否存在
            noteId = "2JAB4GBJ7";
            //查询node  http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]
            //如果node不存在则创建
            //zClient.createNote(notePath);
            // 2J8S5ATKH
            System.out.println("Created note: " + noteId);
            //查询node 信息，sdk ，不存在会抛出异常
            NoteResult renamedNoteResult = zClient.queryNoteResult(noteId);
            System.out.println("Rename note: " + noteId + " name to " + renamedNoteResult.getNotePath());

            //首次提交创建sql paragraph
            //REST API 方式 创建 paragraph  POST http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]/paragraph
            String paragraphId = zClient.addParagraph(noteId, "the first paragraph", "%spark.sql \n" +
                    "SELECT * FROM  ods_qb.ods_woven_base_factory limit 10 ");
            //已存在则更新
            //put  http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]/paragraph/[paragraphId]
            //执行paragraph
            //post http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]/[paragraphId]
            ParagraphResult paragraphResult = zClient.submitParagraph(noteId, paragraphId);
            System.out.println("Added new paragraph and execute it.");
            System.out.println("Paragraph result: " + paragraphResult);
            Thread.sleep(1000*10);
            //结果需要异步获取，status=FINISHED/ERROR时，执行完成
            /***
             * Status
             */
            ParagraphResult result= zClient.queryParagraphResult(noteId,paragraphId);
            System.out.println("查询执行结果："+result);
            //获取结果后，可择机触发删除 Paragraph块，避免nodebook中太多脚本块
            // 删除restApi  DELETE  http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]/[paragraphId]


        } finally {
            if (noteId != null) {
                //zClient.deleteNote(noteId);
                //System.out.println("Note " + noteId + " is deleted");
            }
        }
    }

}

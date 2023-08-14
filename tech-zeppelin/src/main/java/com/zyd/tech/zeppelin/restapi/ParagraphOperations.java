package com.zyd.tech.zeppelin.restapi;

import com.alibaba.fastjson.JSON;
import com.zyd.common.utils.OkHttpUtils;
import jodd.util.concurrent.ThreadFactoryBuilder;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import okhttp3.*;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.client.*;
import com.zyd.tech.zeppelin.utils.ZeppelinClient;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @program: base
 * @description: Paragraph REST API测试
 * @author: zhongyuandong
 * @create: 2023-08-10 14:42:49
 * @Version 1.0
 **/
public class ParagraphOperations {

    public static void main(String[] args) throws Exception {

        String noteId = "2JAB4GBJ7";

//        String userPassword = "bigdata_dev" + ":" + "BS0opayMXf5JRhSg";
//        String encodedAuth = Base64.getEncoder().encodeToString(userPassword.getBytes());
//        System.out.println(encodedAuth);
//        clientApi();

//        clientApi();
//        addParagraph(noteId, 20000);
//        getNote(noteId);
//        clientApi();
//        multiple();
        String str = "2J73SUN7E,2J9FKCYX3,2J8QKXMVW,2J73VDJKT,2J9PVPABD,2J8SVHTVZ,2J7Y7X8Q1,2JA4QMZVT,2JASJ6N43,2J7JWS6D4,2JAAS219M,2J9JPCG6S,2J9N48UY8,2J7BPTDR7,2J9NGJQED,2J9934ZW1,2J86DXP6W,2J9SHDZV1,2J9QKTMWS,2J7HPCUHJ,2JAHESG17,2J9E3KV16,2J8C36ECZ,2J72WXAP8,2J7B78MV5,2JAK2A3M6,2J8V21UMB,2J7F68PK3,2J6VXJJ7W,2JAMW4UF9,2JA8ZUTEN,2J9A5YVKJ,2JA1QUHHG,2J7AW1FBY,2JAMMMSQM,2J7Y1AG6E,2J7RADEZA,2J6UB9MVM,2JANE9B7U,2J9R726H6,2J9GQABVV,2JAB76GNK,2J95H3BBK,2J7A1SBZH,2J6ZWP177,2J8MJYJAA,2JAE8H76B,2J8J41MS7,2JA7T2QBX,2JAGXYXDY,2J948SWEH,2J96K3QTF,2J9QEHMG2,2J7QJZPAT,2J8P7UCWT,2J829AC52,2J9HM39TK,2J9R3UUQX,2J97ZN4NA,2J9RVV6MJ,2J8RHHEJG,2J72A1PUK,2J7SAW6W8,2JAG29SMU,2JAMPD27H,2J9C4PUAM,2JAH7B4HN,2JAQ8A88W,2J9W4Q4S8,2J9T664DG,2J85M29TK,2J8MRMB52,2J98HB6NG,2J7VVG6TF,2J77CGMHJ,2JAQ3YK3K,2JAF716HK,2J8ZDQNUF,2J92EWAPR,2J8PJM6JR,2J8QY37AG,2J9BR4SKJ,2JA3M16VA,2J8H2XGGV,2J8CKY6QB,2J81M6DDD,2J9PHXTCG,2J89TKJYH,2JA9TCCF2,2J8GHRSD3,2J9W8ACEK,2JAJMTBQ9,2J8JSZ4CX,2J73N35NN,2JAKKR5U3,2J9DE91EG,2J9EQ47D7,2J97RMCWK,2JA1FQQUZ,2J93G9364,2J6U83VU9,2J7MTHB5Q,2J8M47X7S,2J8NCG9UM,2J6W61FUW,2J7D3UP6D,2JADV7J11,2J9SMH19E,2J6XFFNZP,2J6VXGEFG,2J92V13TK,2J8Z4DW3Y,2JAPWNYCT,2J7E87Q46,2J8CXE1QJ,2J7XSTE6Q,2JASDS8Q6,2J981EKM6,2J9T924Z2,2J7P6A7EW,2J9BY4TKG,2J82DQG42,2J7D994UE,2J8XCBNVF,2J97EKZUH,2J9HAPEH3,2J9GVYPFA,2J82T97F3,2J9YZW7Z5,2J832DD41,2J9QEPBSD,2J73QB5DF,2J95YPPJ3,2J7JCYBYZ,2J7QF8WM4,2J8CCDWFM,2JAT1WZJC,2J8HMFZ9J,2J6VYVF8Z,2J9Y18F7G,2JA4GRFG2,2J9JJSPSM,2J8BUK7UQ,2J96F24EW,2JA6RAPN1,2J9YF74AR,2J8XX6P5W,2J7A1BZTE,2J91MEZRB,2JA8X58DC,2J8T43SYG,2J8251WM9,2J74KCQYC,2JAEC2KQM,2J962CEB9,2J8UDSUXX,2J9N1KC2W,2J8BW5ZVK,2J9KCZUYS,2J9F5RUCD,2JA1WCY7Y,2J8NYZEPA,2JA2ZSFQX,2JAKMX764,2J8NE8EC9,2J8V6C3X6,2J9RXE5CX,2J7D6SQE3,2JAHY8SCN,2J7YWGCYF,2J9F98BPK,2J9Z2XS94,2J9BJ1N8S,2J95U6FVH,2J9G5T82E,2J9BYV3JC,2JAGPW79J,2J92Z9APF,2J955VJA1,2JAG8Y4XC,2JADYYWUU,2J8J9K9Y7,2JA8UYJWH,2J8NMRUCG,2J9TSS1KQ,2J92JE2S9,2JAA152BV,2J8PHV1VN,2J9NKM79K,2J7HNU9WJ,2J7JVRWVW,2J94QKJF3,2JAM7TWGY,2J9PMX71C,2J8QPWYNB,2J6ZP6KXW,2J6YN4SJ5,2J88WA316,2J8M526VZ,2J84RV2HN,2J9XYTUHY,2J7DQJX9H,2J7WMAWT3,2J9X4AS3R,2J82DV9VX,2J8NKPFBK,2JARXTKXT,2J6ZVY9HD,2J9NEYTT3,2J78H1C7B,2JA9WNGZ7,2J7HNC51C,2J7241MQX,2J9DGGXAQ,2JA96WXAR,2J8VXQ9KP,2J8ZECCXX,2J9PHC1JC,2J9FRUQMB,2J92VYQ7R,2J7XZBDZV,2J8RK18R3,2J8HSTQS7,2JA5615H6,2J8QCT7G6,2J7FPJVPM,2J8WPKCP3,2J86SAS61,2J9Y6EEHT,2JA24QVSV,2JAP28Z5K,2J84PTFRK,2J85VQZ4D,2J7A2TEGK,2J6YYKBC3,2J8KXPXJ5,2J8R6K3W6,2JA9JM183,2J8QHUBMU,2JAPJT32B,2J7Q8E956,2J72H1TNC,2JAJJQHUK,2J8A5KU5X,2J919W8RZ,2J9X8M9A8,2J9UA9RQS,2J97STZHV,2J85V7FRP,2JAF5JKHZ,2J8P5S2RD,2J8EDKT52,2J8DP1XZ2,2J7GXCZEK,2J914GTAK,2J75QVA27,2JAPMUX4U,2JAERYYK9,2J887VJWX,2J7UX516D,2JAAP4CHF,2J9VDCH6M,2JAHH1A9A,2J7SC7QTT,2J8M3UTQN,2JA4JAYR7,2JAGBGAJ4,2JAMN4G43,2J7S6QVFB,2JABE4884,2JA2SGDW5,2JAJ1C4MC,2J86JK47E,2J8JQWPX1,2JAEGDNPN,2J9QR3C38,2JAQR94DX,2J7J66DDJ,2J7V8ENC4,2JAS4SNVP,2J974HS2M,2J8NYRH6J,2JAJXK9XV,2J77C6TNY,2J91H5KAY,2J8KB1GE8,2JA5T73VZ,2JAJTEUK7,2J746N4NN,2JAFSKUV1,2JA1RKZS8,2JA28WX5W,2J983ER3A,2J7Y2WKJB,2J97GVEEZ,2J8AH5ASX,2J877NWTR,2J9DZWE56,2J76XDYU3,2J6W6JNDH";
        String[] parts = str.split(","); // 分割成字符串数组
        List<String> list = Arrays.asList(parts); // 转换成 List 集合
        deleteNoteBook(list);
    }

    private static ExecutorService notebookExecutor = new ThreadPoolExecutor(100, 200, 30, TimeUnit.MINUTES
            , new LinkedBlockingQueue<>(), new ThreadFactoryBuilder().setNameFormat("zeppelin-notebook-callback").get());

    public static void multiple () throws Exception {
        ZeppelinClient zClient = getZClient();
        String text = "%spark.sql \n" +
                "SELECT concat(t.mechine_id,'--', t.status) FROM test.supply_message t LIMIT 100 ";
        //新增note
        List<String> noteIds = createNoteBook(300);
        int count = 0;
        long startTime = System.currentTimeMillis();
        for (String nodeId : noteIds){
            ++count;
            int finalCount = count;
            notebookExecutor.submit(() -> {
                try {
                    String paragraphId = zClient.addParagraph(nodeId, "the first paragraph", text);
                    ParagraphResult paragraphResult = zClient.submitParagraph(nodeId, paragraphId);

                    ParagraphResult result= zClient.queryParagraphResult(nodeId, paragraphId);
                    System.out.println(String.format("查询执行结果%d：%s", finalCount, JSON.toJSONString(result)));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
        long endTime = System.currentTimeMillis();
        System.out.println("耗时：" + (endTime - startTime));
        System.out.println("新增的noteIds：" + StringUtils.join(noteIds, ","));
        //删除note
//        deleteNoteBook(noteIds);
    }

    public static List<String> createNoteBook (int count) throws Exception {
        ZeppelinClient zClient = getZClient();
        List<String> noteIds = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
            String noteId = zClient.createNote("test-notebook-" + i, "spark");
            noteIds.add(noteId);
        }
        return noteIds;
    }

    public static void deleteNoteBook (List<String> noteIds) throws Exception {
        ZeppelinClient zClient = getZClient();
        for (String noteId : noteIds) {
            zClient.deleteNote(noteId);
        }
    }

    public void testHttpUrl () throws IOException {
        OkHttpUtils builder = OkHttpUtils.builder();
        FormBody formBody = new FormBody.Builder()
                .add("param1", "value1")
                .add("param2", "value2")
                .build();
        Request request = new Request.Builder()
                .url("http://uat-bigdata-20-84:8281/api/login")
                .post(formBody)
                .build();
        OkHttpClient client = new OkHttpClient();
        Response response = client.newCall(request).execute();
        System.out.println(request);
        System.out.println(response);
        builder.url("http://uat-bigdata-20-84:8281/api/interpreter/setting").get().async(new OkHttpUtils.ICallBack() {
            @Override
            public void onSuccessful(Call call, String data) {
                System.out.println(data);
            }

            @Override
            public void onFailure(Call call, String errorMsg) {
                System.out.println(errorMsg);
            }
        });
    }

    public static void userRestApi () throws Exception {
        Unirest.config().defaultBaseUrl("http://uat-bigdata-20-84:8281/api");
        HttpResponse<JsonNode> response = Unirest
                .post("/login")
                .field("userName", "admin")
                .field("password", "BuS0FjTHcgbipFa5")
                .asJson();
        if (response.getStatus() != 200) {
            throw new Exception(String.format("Login failed, status: %s, statusText: %s",
                    response.getStatus(),
                    response.getStatusText()));
        }
        String noteId = "2JAB4GBJ7";
        HttpResponse<JsonNode> json = Unirest.get("/notebook/{noteId}")
                .routeParam("noteId", noteId).asJson();
        System.out.println(json.getBody().toString());
    }

    public static void deleteParagraph () throws Exception{
        ClientConfig clientConfig = new ClientConfig("http://uat-bigdata-20-84:8281");
        ZeppelinClient zClient = new ZeppelinClient(clientConfig);
        zClient.login("admin", "BuS0FjTHcgbipFa5");

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

    public static void addParagraph (String noteId, int count) throws Exception {
        ZeppelinClient zClient = getZClient();
        String title = "this is mysql paragraph ";
        String text = "%sql \n" +
                "select concat('zjkj2023aaa')";
        for (int i = 0; i < count; i++) {
            zClient.addParagraph(noteId, title + i, text);
        }
        System.out.println("添加完成！");
    }

    public static ZeppelinClient getZClient () {
        ClientConfig clientConfig = new ClientConfig("http://uat-bigdata-20-84:8281");
        ZeppelinClient zClient = null;
        try {
            zClient = new ZeppelinClient(clientConfig);
            zClient.login("admin", "BuS0FjTHcgbipFa5");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return zClient;
    }

    public static NoteResult getNote (String noteId) throws Exception {
        ZeppelinClient zClient = getZClient();
        NoteResult renamedNoteResult = zClient.queryNoteResult(noteId);
        List<ParagraphResult> paragraphResultList = renamedNoteResult.getParagraphResultList();
        for (int i = 0; i < paragraphResultList.size(); i++) {
            ParagraphResult paragraph = paragraphResultList.get(i);
            zClient.deleteParagraph(noteId, paragraph.getParagraphId());
            if (i%1000 == 0){
                System.out.println("进度：" + i);
            }
        }
        System.out.println("Rename note: " + noteId + " name to " + renamedNoteResult.getNotePath());
        return renamedNoteResult;
    }

    public static void clientApi () throws Exception {

        ZeppelinClient zClient = getZClient();
        String zeppelinVersion = zClient.getVersion();
        System.out.println("Zeppelin version: " + zeppelinVersion);

        String notePath = "/zeppelin_client_examples/note_3";
        String noteId = null;
        try {
            // 判断node是否存在
            noteId = "2JAB4GBJ7";
            //查询node  http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]
            //如果node不存在则创建
            // zClient.createNote(notePath,"spark");
            // zClient.createNote(notePath,"jdbc");

            // 2J8S5ATKH
            System.out.println("Created note: " + noteId);
            //查询node 信息，sdk ，不存在会抛出异常
            NoteResult renamedNoteResult = zClient.queryNoteResult(noteId);
            System.out.println("Rename note: " + noteId + " name to " + renamedNoteResult.getNotePath());

            //首次提交创建sql paragraph
            //REST API 方式 创建 paragraph  POST http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]/paragraph
            String text = "%spark.sql \n" +
                    "SELECT concat(t.mechine_id,'--', t.status) FROM test.supply_message t LIMIT 100 ";
            text = "%sql \n" +
                    "select concat('zjkj2023aaa')";
            String paragraphId = zClient.addParagraph(noteId, "the first paragraph", text);
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
            ParagraphResult result= zClient.queryParagraphResult(noteId, paragraphId);
            System.out.println("查询执行结果："+result);

            //删除paragraph
            zClient.deleteParagraph(noteId, paragraphId);
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

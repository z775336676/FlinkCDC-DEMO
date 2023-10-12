package com.example.caiflinkcdc.func;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.example.caiflinkcdc.util.DateUtils;
import com.example.caiflinkcdc.util.DstJDBCUtils;
import com.example.caiflinkcdc.util.SrcJDBCUtils;
import com.example.caiflinkcdc.util.StdoutUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

/**
 * @author 77533
 */
public class ETLKbossWindowFunction extends ProcessAllWindowFunction<JSONObject, Object, TimeWindow> {

    private final static Logger log = LoggerFactory.getLogger(ETLKbossWindowFunction.class);
    private PreparedStatement cusps = null;
    private PreparedStatement cususerps = null;
    private PreparedStatement hisps = null;
    private int cusi = 0;
    private int cususeri = 0;
    private int hisi = 0;
    private int cuscount = 0;
    private int cususercount = 0;
    private int hiscount = 0;

    private final int batchNum = 200;

    @Override
    public void process(ProcessAllWindowFunction<JSONObject, Object, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<Object> collector) throws Exception {
        long startTime = System.currentTimeMillis();
        //初始化
        init();
        StdoutUtils.print("window process start..");
        //数据清洗
        dataETL(iterable);
        StdoutUtils.print("已同步KBOSS客户数据："+cuscount+"条");
        StdoutUtils.print("已同步KBOSS客户跟进人数据："+cususercount+"条");
        StdoutUtils.print("已插入历史数据："+hiscount+"条");
        long endTime =  System.currentTimeMillis();
        long usedTime = (endTime-startTime)/1000;
        StdoutUtils.print("window process end,used time:"+usedTime+"s");
    }

    private void dataETL(Iterable<JSONObject> iterable) throws Exception {

        for (JSONObject jsonObject : iterable) {
            //操作类型
            //String op = jsonObject.getString("op");
            //数据库名
            //String db = jsonObject.getString("db");
            //表名
            String tb = jsonObject.getString("tb").toLowerCase(Locale.ROOT);
            JSONObject after = jsonObject.getJSONObject("after");
            //JSONObject before = jsonObject.getJSONObject("before");
            String customerId;
            boolean isInsert = false;
            //判断表名，防止意外情况
            if (tb.toLowerCase(Locale.ROOT).contains("uf_customerinfo")) {
                String phone = after.getString("phone");
                Integer type = after.getInteger("type");
                //个人客户数据
                if (type!=null && type == 0 && StringUtils.isNotBlank(phone) && phone.length()<=11) {
                    //查询BOSS中是否存在客户数据
                    JSONObject customer = DstJDBCUtils.queryForMap("select id,create_time,update_time,is_delete,create_user_id,update_user_id,customer_no,type,level,name,sex,phone,id_card,contract_name,position,we_chat,qq,e_mail,fax,credit_code,industry_id,enterprise_scale_id,source,province_code,city_code,address,origin from ng_customer where phone = ?", after.getString("phone"));
                    if (customer == null) {
                        //不存在时，插入数据
                        customerId = replaceCustomer(after,null);
                        isInsert = true;
                    } else {
                        //存在时判断origin，OA和KBOSS过来的数据才同步更新，BOSS原有数据不动
                        if("OA".equals(customer.getString("origin")) || "KBOSS".equals(customer.getString("origin"))){
                            //更新数据
                            customerId = replaceCustomer(after,customer);
                        }else{
                            StdoutUtils.print("continue 1");
                            continue;
                        }
                    }
                //企业客户数据
                } else if (type!=null && type == 1 && StringUtils.isNotBlank(phone) && phone.length()<=11) {
                    if (StringUtils.isNotBlank(after.getString("creditcode"))) {
                        //查询BOSS中是否存在客户数据
                        JSONObject customer = DstJDBCUtils.queryForMap("select id from ng_customer where credit_code = ?", after.getString("creditcode"));
                        if (customer == null) {
                            //不存在时，插入数据
                            customerId = replaceCustomer(after,null);
                            isInsert = true;
                        } else {
                            //存在时判断origin，OA和KBOSS过来的数据才同步更新，BOSS原有数据不动
                            if("OA".equals(customer.getString("origin")) || "KBOSS".equals(customer.getString("origin"))){
                                //更新数据
                                customerId = replaceCustomer(after,customer);
                            }else{
                                StdoutUtils.print("continue 2");
                                continue;
                            }
                        }
                    } else {
                        //没有企业代码则丢弃数据
                        StdoutUtils.print("continue 3");
                        continue;
                    }
                } else {
                    //其他情况丢弃数据
                    StdoutUtils.print("continue 4");
                    continue;
                }

                if(customerId!=null){
                    //查询KBOSS中的客户所属人
                    JSONObject user = SrcJDBCUtils.queryForMap("select ID,LASTNAME,WORKCODE from hrmresource where id = ?", after.getString("modedatacreater"));
                    //查询KBOSS中客户对应商机所属人
                    JSONArray busers = SrcJDBCUtils.queryForList("select distinct busi.businessowner,hrs.WORKCODE from uf_customerinfo cus join uf_businessopportunityinformation busi on cus.id = busi.customername join hrmresource hrs on hrs.id = busi.businessowner where cus.id = ?", after.getString("id"));
                    //排重
                    if(user!=null){
                        boolean flag = false;
                        for (int i = 0; i < busers.size(); i++) {
                            JSONObject bu = busers.getJSONObject(i);
                            if(bu.getString("businessowner").equals(user.getString("ID"))){
                                flag = true;
                            }
                        }
                        if(!flag){
                            JSONObject u = new JSONObject();
                            u.put("businessowner",user.getString("ID"));
                            u.put("WORKCODE",user.getString("WORKCODE"));
                            busers.add(u);
                        }
                    }
                    for (int i = 0; i < busers.size(); i++) {
                        JSONObject buser = busers.getJSONObject(i);
                        //插入的情况，直接插入
                        if(isInsert){
                            insertCusUser(buser,customerId,after.getInteger("type"));
                        }else{//更新的情况
                            //判断BOSS中所属人是否存在
                            JSONObject bu = DstJDBCUtils.queryForMap("select a.user_id from ng_customer_for_user_connect a join ng_customer b on a.customer_id = b.id join ng_kboss_auth.ng_kb_user c on a.user_id = c.ng_user_id " +
                                    "where c.work_code = ? and a.customer_id = ?", buser.getString("WORKCODE"),customerId);
                            //不存在时，插入数据
                            if(bu==null){
                                insertCusUser(buser,customerId,after.getInteger("type"));
                            }
                        }
                    }
                }
            }
        }
        if(cusi>0){
            cusps.executeBatch();
        }
        if(cususeri>0){
            cususerps.executeBatch();
        }
        if(hisi>0){
            hisps.executeBatch();
        }
    }

    private void init() throws SQLException {
        //获取全局变量
        ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globConf = (Configuration) globalParams;
        String src = globConf.getString("src", null);
        StdoutUtils.init(src);
        SrcJDBCUtils.init(src);
        String cussql = "replace into ng_customer(id,create_time,update_time,is_delete,customer_no,type,name,phone,contract_name,we_chat,qq,e_mail,credit_code,origin,address,province_code,city_code,source) " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        cusps = DstJDBCUtils.getConnection().prepareStatement(cussql);

        String cususersql = "insert into ng_customer_for_user_connect(id,create_time,update_time,is_delete,customer_id,user_id) " +
                "values (?,?,?,?,?,?)";
        cususerps = DstJDBCUtils.getConnection().prepareStatement(cususersql);

        String hissql = "insert into ng_customer_history(source,update_time,update_id,customer_id,`before`,`after`) " +
                "values(?,?,?,?,?,?)";
        hisps = DstJDBCUtils.getConnection().prepareStatement(hissql);
    }

    private String replaceCustomer(JSONObject after, JSONObject customer) throws SQLException {
        String oaCity = after.getString("address");
        String provinceCode = null;
        String cityCode = null;
        if(StringUtils.isNotBlank(oaCity)){
            //查询区域信息
            JSONObject city = SrcJDBCUtils.queryForMap("select id,provinceid from hrmcity where id = ?", oaCity);
            if(city!=null){
                provinceCode = city.getString("PROVINCEID");
                cityCode = city.getString("ID");
            }
        }
        //计数
        cusi++;
        cuscount++;
        String now = DateUtils.getTime();
        String head;
        String cuHead;
        String type;
        String contractName = null;
        String creditCode = null;
        String id;
        //KBOSS个人客户，ID以21开头，KBOSS企业客户，ID以22开头
        if(after.getInteger("type") == 0){
            head = "21";
            cuHead = "KH-21";
            type = "PERSONAL";
        }else{
            head = "22";
            cuHead = "KH-22";
            type = "ENTERPRISE";
            contractName = after.getString("lxr");
            creditCode = after.getString("creditcode");
        }
        //customer为空时，生成ID，插入数据
        if(customer == null){
            id = head + StringUtils.leftPad(after.getString("id"),16,'0');
            String customerNo = cuHead + StringUtils.leftPad(after.getString("id"),11,'0');
            cusps.setString(1, id);
            cusps.setString(2, now);
            cusps.setString(3,now);
            cusps.setInt(4,0);
            cusps.setString(5,customerNo);
            cusps.setString(6,type);
        }else {//更新数据
            id = customer.getString("id");
            cusps.setString(1, id);
            cusps.setString(2, customer.getString("create_time"));
            cusps.setString(3,now);
            cusps.setInt(4,customer.getInteger("is_delete"));
            cusps.setString(5,customer.getString("customer_no"));
            cusps.setString(6,customer.getString("type"));
            //插入更新记录
            insertHistory(after,customer,contractName,creditCode,provinceCode,cityCode);
        }
        cusps.setString(7,StringUtils.substring(after.getString("customername"),0,64));
        cusps.setString(8,after.getString("phone"));
        cusps.setString(9,StringUtils.substring(contractName,0,32));
        cusps.setString(10,StringUtils.substring(after.getString("wechat"),0,32));
        cusps.setString(11,StringUtils.substring(after.getString("qqnumber"),0,32));
        cusps.setString(12,StringUtils.substring(after.getString("customermail"),0,32));
        cusps.setString(13,StringUtils.substring(creditCode,0,18));
        cusps.setString(14,"KBOSS");
        cusps.setString(15,StringUtils.substring(after.getString("jddz"),0,64));
        cusps.setString(16,provinceCode);
        cusps.setString(17,cityCode);
        cusps.setString(18,"SELF");
        cusps.addBatch();
        if(cusi>batchNum){
            cusps.executeBatch();
            cusps.clearBatch();
            cusi = 0;
        }
        return id;
    }

    private void insertHistory(JSONObject after, JSONObject customer, String contractName, String creditCode, String provinceCode, String cityCode) throws SQLException {
        hisi++;
        hiscount++;
        //获取更新人
        String modedatamodifier = after.getString("modedatamodifier");
        String modedatacreater = after.getString("modedatacreater");
        if(StringUtils.isBlank(modedatamodifier)){//更新人数据为空，则使用创建人
            modedatamodifier = modedatacreater;
        }
        String beforeCus = customer.toJSONString();
        customer.put("name",StringUtils.substring(after.getString("customername"),0,64));
        customer.put("phone",after.getString("phone"));
        customer.put("contract_name",StringUtils.substring(contractName,0,32));
        customer.put("we_chat",StringUtils.substring(after.getString("wechat"),0,32));
        customer.put("qq",StringUtils.substring(after.getString("qqnumber"),0,32));
        customer.put("e_mail",StringUtils.substring(after.getString("customermail"),0,32));
        customer.put("credit_code",StringUtils.substring(creditCode,0,18));
        customer.put("source","SELF");
        customer.put("origin","KBOSS");
        customer.put("address",StringUtils.substring(after.getString("jddz"),0,64));
        customer.put("province_code",provinceCode);
        customer.put("city_code",cityCode);
        String afterCus = customer.toJSONString();
        hisps.setString(1,"KBOSS");
        hisps.setString(2,DateUtils.getTime());
        hisps.setString(3,modedatamodifier);
        hisps.setString(4,customer.getString("id"));
        hisps.setString(5,beforeCus);
        hisps.setString(6,afterCus);
        hisps.addBatch();
        if(hisi>batchNum){
            hisps.executeBatch();
            hisps.clearBatch();
            hisi = 0;
        }
    }

    private void insertCusUser(JSONObject user, String customerId,Integer type) throws SQLException {
        //查询BOSS中所属人信息
        JSONObject bu = DstJDBCUtils.queryForMap("select ng_user_id from ng_kboss_auth.ng_kb_user " +
                "where work_code = ? ", user.getString("WORKCODE"));
        if(bu!=null){
            String now = DateUtils.getTime();
            //计数
            cususeri++;
            cususercount++;
            String head;
            //KBOSS个人客户，ID以21开头，KBOSS企业客户，ID以22开头
            if(type == 0){
                head = "21";
            }else{
                head = "22";
            }
            //id生成规则head+userId后4位+时间戳
            String id = head + StringUtils.leftPad(String.valueOf(DateUtils.getNowDate().getTime()),16,'0');
            cususerps.setString(1,id);
            cususerps.setString(2,now);
            cususerps.setString(3,now);
            cususerps.setInt(4,0);
            cususerps.setString(5,customerId);
            cususerps.setString(6,bu.getString("ng_user_id"));
            cususerps.addBatch();
            if(cususeri>batchNum){
                cususerps.executeBatch();
                cususerps.clearBatch();
                cususeri = 0;
            }
        }
    }

    @Override
    public void close() throws Exception {
        Connection conn = DstJDBCUtils.getConnection();
        Connection conn2 = SrcJDBCUtils.getConnection();
        if (conn != null) {
            conn.close();
        }
        if (conn2 != null) {
            conn2.close();
        }

    }

}

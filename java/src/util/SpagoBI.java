package util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

public class SpagoBI {

   public static String getQuery(Connection connBI, String dataset, String businessModel) throws Exception {

      String user = "biadmin";
      String password = "biadmin";

      PreparedStatement ps = null;
      ResultSet rs = null;

      try {
         String sql = "select configuration from sbi_data_set "
               + "where label = ? and active = 1";
         ps = connBI.prepareStatement(sql);
         ps.setString(1, dataset);

         rs = ps.executeQuery();

         if(rs.next()) {
            String configuration = rs.getString("configuration");
            System.out.println("Configuration: " + configuration);

            String type = getDatasetType(configuration);

            if(type.equals("qbe")) {
               String cookieSpagoBI = getCookieSpagoBI();
               System.out.println("Cookie SpagoBI: " + cookieSpagoBI);

               if(cookieSpagoBI != null) {
                  if(login(cookieSpagoBI, user, password)) {

                     String cookieSpagoBIQbe = getCookieSpagoBIQbeEngine(cookieSpagoBI, businessModel);
                     System.out.println("Cookie QbeEngine: " + cookieSpagoBIQbe);

                     if(cookieSpagoBIQbe != null) {

                        String catalogue = getCatalogue(configuration);
                        System.out.println("Catalogue: " + catalogue);

                        String sqlJson = getSql(cookieSpagoBIQbe, catalogue);
                        System.out.println("SqlJson: " + sqlJson);

                        JsonReader reader1 = Json.createReader(new StringReader(sqlJson));
                        JsonObject obj1 = reader1.readObject();

                        String query = obj1.getString("sql");

                        return query;
                     }
                  }
               }
            } else if(type.equals("query")) {
               JsonReader reader1 = Json.createReader(new StringReader(configuration));
               JsonObject obj1 = reader1.readObject();

               String query = obj1.getString("Query");
               
               return query + "_jdbc:mysql://localhost:3306/bi_bi";
            }
         }

      } catch(Exception e) {
         throw e;
      } finally {
         Conexao.close(rs, ps);
      }

      throw new Exception("Erro ao buscar a query");
   }
   
   public static String getDatasetType(String configuration) throws Exception {
      JsonReader reader1 = Json.createReader(new StringReader(configuration));
      JsonObject obj1 = reader1.readObject();

      if(obj1.containsKey("qbeJSONQuery")) {
         return "qbe";
      } else if(obj1.containsKey("Query")) {
         return "query";
      }
      
      throw new Exception("DatasetType nao encontrado");
   }

   public static boolean limpaCacheBySignature(Connection conn, String signature) throws Exception {
      String sql = "select table_name from sbi_cache_item where signature = ?";

      PreparedStatement ps = conn.prepareStatement(sql);
      ps.setString(1, signature);

      ResultSet rs = ps.executeQuery();

      if(rs.next()) {
         String tableName = rs.getString("table_name");

         sql = "drop table " + tableName;
         ps = conn.prepareStatement(sql);
         ps.execute();

         sql = "delete from sbi_cache_item where signature = ?";
         ps = conn.prepareStatement(sql);
         ps.setString(1, signature);
         ps.execute();

         return true;
      } else {
         throw new Exception("Nao foi possivel limpar o cache: " + signature);   
      }
   }

   private static String getCookieSpagoBI() throws Exception {
      String uri = "http://192.168.2.134:8080/SpagoBI/";
      URL url = new URL(uri);

      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");

      if(conn.getResponseCode() == 200) {
         String cookie = conn.getHeaderField("Set-Cookie").replaceAll("; Path=/SpagoBI; HttpOnly", "");

         conn.disconnect();
         return cookie;
      } else {
         conn.disconnect();
         throw new Exception("Nao foi possivel obter o cookie de sessao do SpagoBI- code: " + conn.getResponseCode());  
      }
   }

   private static boolean login(String cookieSpagoBI, String user, String password) throws Exception {
      String uri = "http://192.168.2.134:8080/SpagoBI/servlet/AdapterHTTP?PAGE=LoginPage&NEW_SESSION=TRUE";
      URL url = new URL(uri);

      String urlParameters = "isInternalSecurity=true&userID=" + user + "&password=" + password + "&x=0&y=0";
      byte[] postData = urlParameters.getBytes(StandardCharsets.UTF_8);

      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setDoOutput(true);
      conn.setInstanceFollowRedirects(false);
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      conn.setRequestProperty("Referer", uri);
      conn.setRequestProperty("Content-Length", Integer.toString(postData.length));
      conn.setRequestProperty("Cookie", cookieSpagoBI);
      conn.setUseCaches(false);

      conn.getOutputStream().write(postData);

      if(conn.getResponseCode() == 200) {
         conn.disconnect();
         return true;
      } else {
         conn.disconnect();
         throw new Exception("Nao foi possivel efetuar login no SpagoBI - code: " + conn.getResponseCode());
      }
   }

   private static String getCookieSpagoBIQbeEngine(String cookieSpagoBI, String businessModel) throws Exception {
      String uri = "http://192.168.2.134:8080/SpagoBIQbeEngine/servlet/AdapterHTTP?ACTION_NAME=BUILD_QBE_DATASET_START_ACTION&user_id=biadmin&NEW_SESSION=TRUE&SBI_LANGUAGE=en&SBI_COUNTRY=US&DATASOURCE_LABEL=bi&DATAMART_NAME=" + businessModel;
      URL url = new URL(uri);

      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Referer", "http://192.168.2.134:8080/SpagoBI/servlet/AdapterHTTP?ACTION_NAME=MANAGE_DATASETS_ACTION&LIGHT_NAVIGATOR_RESET_INSERT=TRUE");

      if(conn.getResponseCode() == 200) {
         String cookie = conn.getHeaderField("Set-Cookie").replaceAll("; Path=/SpagoBIQbeEngine; HttpOnly", "");

         conn.disconnect();
         return cookie;
         
      } else {
         conn.disconnect();
         throw new Exception("Nao foi possivel conectar ao SpagoBIQbeEngine - code: " + conn.getResponseCode());
      }
   }

   private static String getSql(String cookie, String catalogue) throws Exception {
      //SET catalogue
      String urlParameters = "catalogue=" + catalogue
            + "&currentQueryId=q1"
            + "&ambiguousFieldsPaths=[]"
            + "&ambiguousRoles=[]";
      byte[] postData = urlParameters.getBytes(StandardCharsets.UTF_8);

      String uri1 = "http://192.168.2.134:8080/SpagoBIQbeEngine/servlet/AdapterHTTP?ACTION_NAME=SET_CATALOGUE_ACTION";
      URL url1 = new URL(uri1);
      HttpURLConnection conn1 = (HttpURLConnection) url1.openConnection();           
      conn1.setDoOutput(true);
      conn1.setInstanceFollowRedirects(false);
      conn1.setRequestMethod("POST");
      conn1.setRequestProperty("X-Requested-With", "XMLHttpRequest");
      conn1.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
      conn1.setRequestProperty("charset", "utf-8");
      conn1.setRequestProperty("Content-Length", Integer.toString(postData.length));
      //conn4.setRequestProperty("Referer", "http://192.168.2.134:8080/SpagoBIQbeEngine/servlet/AdapterHTTP?ACTION_NAME=BUILD_QBE_DATASET_START_ACTION&user_id=biadmin&NEW_SESSION=TRUE&SBI_LANGUAGE=en&SBI_COUNTRY=US&DATASOURCE_LABEL=bi&DATAMART_NAME=BM_HABILITACAO");
      conn1.setRequestProperty("Cookie", cookie);
      conn1.setUseCaches(false);

      conn1.getOutputStream().write(postData);

      conn1.getInputStream();

      /*Reader in1 = new BufferedReader(new InputStreamReader(conn1.getInputStream(), "UTF-8"));
      for(int i; (i = in1.read()) >= 0;) {
         System.out.print((char) i);
      }*/

      conn1.disconnect();

      //GET catalogue
      urlParameters = "replaceParametersWithQuestion=true";
      postData = urlParameters.getBytes(StandardCharsets.UTF_8);

      String uri2 = "http://192.168.2.134:8080/SpagoBIQbeEngine/servlet/AdapterHTTP?ACTION_NAME=GET_SQL_QUERY_ACTION";
      URL url2 = new URL(uri2);
      HttpURLConnection conn2 = (HttpURLConnection) url2.openConnection();           
      conn2.setDoOutput(true);
      conn2.setInstanceFollowRedirects(false);
      conn2.setRequestMethod("POST");
      conn2.setRequestProperty("X-Requested-With", "XMLHttpRequest");
      conn2.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
      conn2.setRequestProperty("charset", "utf-8");
      conn2.setRequestProperty("Content-Length", Integer.toString(postData.length));
      //conn5.setRequestProperty("Referer", "http://192.168.2.134:8080/SpagoBIQbeEngine/servlet/AdapterHTTP?ACTION_NAME=BUILD_QBE_DATASET_START_ACTION&user_id=biadmin&NEW_SESSION=TRUE&SBI_LANGUAGE=en&SBI_COUNTRY=US&DATASOURCE_LABEL=bi&DATAMART_NAME=BM_HABILITACAO");
      conn2.setRequestProperty("Cookie", cookie);
      conn2.setUseCaches(false);

      conn2.getOutputStream().write(postData);

      Reader in2 = new BufferedReader(new InputStreamReader(conn2.getInputStream(), "UTF-8"));
      String resposta = "";
      for(int i; (i = in2.read()) >= 0;) {
         resposta += (char) i;
      }

      conn2.disconnect();
      return resposta;
   }

   private static String getCatalogue(String configuration) throws Exception {
      //-----------------------------
      JsonReader reader1 = Json.createReader(new StringReader(configuration));
      JsonObject obj1 = reader1.readObject();

      String qbeJSONQuery = obj1.getString("qbeJSONQuery");
      //System.out.println(qbeJSONQuery);

      //-----------------------------
      JsonReader reader2 = Json.createReader(new StringReader(qbeJSONQuery));
      JsonObject obj2 = reader2.readObject().getJsonObject("catalogue");

      JsonArray jsonArray = obj2.getJsonArray("queries");

      return jsonArray.toString();
   }

   public static String getHashedSignature(String signature) throws Exception {
      MessageDigest messageDigest;
      
      messageDigest = MessageDigest.getInstance("SHA-256");
      messageDigest.update(signature.getBytes("UTF-8"));

      // convert the byte to hex format method 1
      byte byteData[] = messageDigest.digest();
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < byteData.length; i++) {
         sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
      }

      return sb.toString();

   }
}


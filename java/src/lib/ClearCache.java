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

public class ClearCache {

   public static boolean clear(String dataset, String businessModel) throws Exception {
      Connection connBI = new Conexao("SpagoBI").getConnection();

      PreparedStatement ps = null;
      ResultSet rs = null;

      try {
         String sql = "select configuration from sbi_data_set "
               + "where label = ? and active = 1";
         ps = connBI.prepareStatement(sql);
         ps.setString(1, dataset);

         rs = ps.executeQuery();

         if(rs.next()) {
            String query = getQuery(connBI, dataset, businessModel);
            System.out.println("Query: " + query);

            if(query != null) {
               String signature = getHashedSignature(query);
               System.out.println("Signature: " + signature);

               if(signature != null) {
                  if(limpaCacheBySignature(connBI, signature)) {
                     return true;
                  }  
               }
            }
         }

      } catch(Exception e) {
         throw e;
      } finally {
         Conexao.close(rs, ps, connBI);
      }
      
      throw new Exception("error cleaning cache");
   }
}


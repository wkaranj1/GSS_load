/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gss_load;

import static gss_load.data_load.csvFile;
import static gss_load.data_load.delimitor;
import static gss_load.data_load.getConnection;
import static gss_load.data_load.getHeaders;
import static java.nio.file.Files.list;
import java.util.*;
import java.sql.*;
import static java.util.Collections.list;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.Connection;

  
/**
 *
 * @author wilson.karanja
 */
public class GSS_load {

        public static List getHeaders() throws FileNotFoundException{
            int iteration = 0;
            String csvFile = "C:\\Users\\wilson.karanja\\Desktop\\java.csv";
            BufferedReader reader = new BufferedReader(new FileReader(csvFile));
            List<String> lines = new ArrayList<>();
            List<String> headers = new ArrayList<String>();
            String line = null;
            try {
                line = reader.readLine(); 
                      
                    lines.add(line);
                           } catch (IOException ex) {
                Logger.getLogger(GSS_load.class.getName()).log(Level.SEVERE, null, ex);
            }
             headers = Arrays.asList(lines.get(0).split(","));;
             System.out.println(lines.get(0));
            
             return headers;   
    }
        public static Connection getConnection(){
        String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
        String DB_URL = "jdbc:mysql://localhost:3306";
        String USER = "root";
        String PASS = "password";
        String b = null;
        
         Connection conn = null;

            try{
             Class.forName("com.mysql.jdbc.Driver");
             System.out.println("Connecting to a selected database...");
             conn = DriverManager.getConnection(DB_URL, USER, PASS);
              b = "Connected database successfully...";
             
            
        }   catch (ClassNotFoundException ex) {
                Logger.getLogger(GSS_load.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SQLException ex) {
                Logger.getLogger(GSS_load.class.getName()).log(Level.SEVERE, null, ex);
            }
           return conn;
        }
        public int readFile(){
            String csvFile = "C:\\Users\\wilson.karanja\\Desktop\\java.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] country = line.split(cvsSplitBy);

                System.out.println("Country [code= " + country[4] + " , name=" + country[5] + "]");

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
            
            return 1;
        }
        
        public static void createColumns(){
            Statement stmt = null;
            ResultSet rs = null;
            List<String> list = new ArrayList<String>();
            try {
                 list = getHeaders();
                System.out.println(list.get(0));
                } catch (FileNotFoundException ex) {
                Logger.getLogger(GSS_load.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                stmt = getConnection().prepareStatement("ALTER TABLE \"table_name\"\n" +
"Change \"column 1\" \"column 2\" [\"Data Type\"];");
            } catch (SQLException ex) {
                Logger.getLogger(GSS_load.class.getName()).log(Level.SEVERE, null, ex);
            }
            
        }
         public static TreeMap loadFile() throws FileNotFoundException, SQLException{
             TreeMap<Integer, List> map = new TreeMap<>();
           Connection  conn1 = getConnection();
        int columns = getHeaders().size();
        List cols = new ArrayList<String>();
        for(int i = 0; i <= columns; i ++){
            cols.add("col" + i);
        }
        BufferedReader br = null;
        String line = null;
        String cvsSplitBy = ",";
       int iteration = 0;
         
            //while (iteration < columns){
        try {
           
            
            br = new BufferedReader(new FileReader(csvFile));
            int s = 0;
            while ((line = br.readLine()) != null) {
               List<String> lst = null;
                String[] val = line.split(cvsSplitBy);
                lst = Arrays.asList(val);
                map.put( s++, lst);

              //  PreparedStatement pst = conn1.prepareStatement("insert into gss_reference.file_load("+cols.get(iteration)+") values('"+val[s]+"');");
             //  pst.executeUpdate();
                
          }
            s++;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        
       // }
        //iteration++;
      }
            return map;
         }
         
         
         public static void insertVals() throws FileNotFoundException, SQLException{
           Connection  conn1 = getConnection();
             TreeMap<Integer, List> a = loadFile();
             for (int k = 1; k< a.size(); k ++){
             String col = "";
             String val = "";
             List<String> cols = a.get(k);
             for (int i = 0; i< cols.size(); i ++){
                 if(i < cols.size() - 1)
                 col += " col" + i + ",";
                 else
                     col += " col" + i + ""; 
             }
             int last = 0;
             for(String  s: cols){
              if(last < cols.size() - 1) {  
             val += "'" + s + "',"; 
             }
             else {
                 val += "'" + s + "'";  
                   }    
              last ++;
             }
             
               PreparedStatement pst = conn1.prepareStatement("insert into gss_reference.file_load("+col + ") values  (" + val +");");
               pst.executeUpdate();
             System.out.println("("+col + ") values  (" + val +");");
             }
         }
            
          public static void main(String[] args) throws SQLException {
        
            try {
                List<String> list = getHeaders();
                System.out.println(list.get(0));
                loadFile();
                System.out.println(getConnection());
                insertVals();
                
                    TreeMap<Integer, List> a = loadFile();
                for(Integer b: a.keySet()){
                     System.out.println( b +"  " + a.get(b).toString());
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(GSS_load.class.getName()).log(Level.SEVERE, null, ex);
            }
            

    }
    }
    


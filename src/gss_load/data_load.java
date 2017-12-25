/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gss_load;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author wilson.karanja
 */
public class data_load {
    
    public static Connection conn = null;
    public static String delimitor = null;
    public static String csvFile = "C:\\Users\\wilson.karanja\\Desktop\\java.csv";
    
    public static void setDelimitor(String del){
        delimitor = del;
    }
    public static List getHeaders() throws FileNotFoundException{
            int iteration = 0;
            
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
             headers = Arrays.asList(lines.get(0).split(delimitor));
             System.out.println(lines.get(0));
            
             return headers;   
    }
        public static Connection getConnection(){
        String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
        String DB_URL = "jdbc:mysql://localhost:3306";
        String USER = "root";
        String PASS = "password";
        String b = null;
        
       

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
       
        public static void colDataLoad(String protocol, String vendor, String database_col, int col_position, String file_col_value, String constant){
                
        try {
           
             PreparedStatement pst = conn.prepareStatement("insert into gss_reference.column_values (study_protocol_id, vendor_code, Database_column, File_col_position, file_col_value, constant_val)"
                     + " values ('"+protocol +"', '"+vendor +"', '"+database_col +"', "+col_position +", '"+file_col_value +"', '"+constant+"');"); 
             pst.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(data_load.class.getName()).log(Level.SEVERE, null, ex);
        }
            
        }
        
        public static List getColumns(){
            Statement stmt = null;
            ResultSet rs = null;
            List<String> list2 = new ArrayList<String>();
             
        try {
            stmt = getConnection().createStatement();
             rs = stmt.executeQuery("select column_name from gss_reference.dts_database_cols;"); 
            while(rs.next()){
                list2.add(rs.getString(1));
                }
        } catch (SQLException ex) {
            Logger.getLogger(data_load.class.getName()).log(Level.SEVERE, null, ex);
        }


            return list2;
            
            
        }
        
        public  static String validateCol(String sp, String ven, String colVal, int pos, String match, String cnst){
            
                 Statement stmt = null;
                 ResultSet rs = null;
                 List<String> list = new ArrayList<String>();;
          
             
        try {
            stmt = getConnection().createStatement();
             rs = stmt.executeQuery("select * from gss_reference.column_values where study_protocol_id = '"+sp+"' and vendor_code = '"+ven+"' and File_col_position = "+pos+" and file_col_value = '"+match+"';"); 
           while( rs.next()){
               list.add(rs.getString(1) + rs.getString(2) + rs.getString(3));
           }                    
               
        } catch (SQLException ex) {
            Logger.getLogger(data_load.class.getName()).log(Level.SEVERE, null, ex);
        }
            if(list.isEmpty()){
                return "nil";
            }
            else{
                return "exists";
            }
        }
        
        public static void addDelimitor(String sp, String ven, String name, String val){
            
              try {
           
             PreparedStatement pst = getConnection().prepareStatement("insert into gss_reference.delimitors (study_protocol_id, vendor, delim_name, delimiter_value)"
                     + " values ('"+sp +"', '"+ven +"', '"+name +"', '"+val+"');"); 
             pst.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(data_load.class.getName()).log(Level.SEVERE, null, ex);
        }
            
       }
        
         public static int loadFile() throws FileNotFoundException, SQLException{
        int columns = getHeaders().size();
        List cols = new ArrayList<String>();
        for(int i = 0; i <= columns; i ++){
            cols.add("col" + i);
        }
        BufferedReader br = null;
        String line = null;
        String cvsSplitBy = ",";
       int iteration = 0;
          System.out.println("running");
            
        try {
            for(int k = 0; k <= columns; k ++){
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                if(iteration > 0){
                // use comma as separator
                String[] country = line.split(cvsSplitBy);

                PreparedStatement pst = getConnection().prepareStatement("insert into gss_reference.file_load("+cols.get(1)+") values('"+country[k]+"');");
               pst.executeUpdate();
                        
                }
                 iteration ++;
               }
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
            return 0;
         }
           
}


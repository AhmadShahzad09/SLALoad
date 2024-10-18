package com.start.load.application.execution;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import com.start.load.entity.Load;
import com.start.load.entity.MeterStaging;
import com.start.load.repository.LoadRepository;

/**
 * @author Roger Manzo
 * @apiNote Service Map
 */
@Configuration
public class ScheduleServicePostgre {

    @Value("${spring.datasource.url}")
    private String databaseInt;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;
    @Value("${spring.datasource.url2}")
    private String databaseExtern;
    @Value("${spring.datasource.username2}")
    private String username2;
    @Value("${spring.datasource.password2}")
    private String password2;
    @Value("${spring.datasource.schedule.postgre}")
    private String postgre;
    @Value("${spring.datasource.schedule.truncate}")
    private String truncate;

    @Value("addc")
    private String addc;

    @Value("aadc")
    private String aadc;

    @Value("${spring.datasource.tables.tablemeterstaging}")
    private String tableMeterStaging;



    @Value("${spring.datasource.schedule.truncateonlytwo}")
    private String tableOnlyTwoDays;

    @Value("${spring.datasource.monthly}")
    private String monthlyTruncate;

    @Value("${spring.datasource.timezone}")
    private String timezone;
    
    private static final Logger logger = LogManager.getLogger(ScheduleServicePostgre.class);

    @Autowired
    private LoadRepository loadRepository;

    public static boolean isNull(BigDecimal bdg) {
        if (bdg == null) {
            return true;
        }
        return false;
    }

//    @Scheduled(cron = "${spring.datasource.schedule.truncate}", zone = "${spring.datasource.timezone}")
//    public void truncatedb() {
//        Statement stmt;
//        try {
//            Class.forName("org.postgresql.Driver");
//            Connection conn = DriverManager.getConnection(databaseInt, username, password);
//            if (conn != null) {
//                logger.info("\u001B[32m" + "Connection established" + " \u001B[0m");
//            } else {
//                logger.info("\u001B[31m" + "Connection failed" + " \u001B[0m");
//            }
//
//            String querycommand = String.format("Delete from " + tableCommandStaging
//                    + " where datetime < now() - interval '" + monthlyTruncate + "'");
//
//            String querymeter = String.format("Delete from " + tableMeterStaging
//                    + " where process_date < now() - interval '" + monthlyTruncate + "'");
//
//            assert conn != null;
//            stmt = conn.createStatement();
//            stmt.executeUpdate(querycommand);
//            stmt.executeUpdate(querymeter);
//            logger.info("\u001B[32m" + "Truncate completed successfully" + " \u001B[0m");
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }

    private void truncatedb(Date startDate, Date endDate) {
        Statement stmt;
        Connection conn=null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(databaseInt, username, password);
            if (conn != null) {
                logger.info("\u001B[32m" + "Connection established" + " \u001B[0m");
            } else {
                logger.info("\u001B[31m" + "Connection failed" + " \u001B[0m");
            }

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            String querymeter = String.format(
                    "DELETE FROM " + tableMeterStaging + " WHERE process_date BETWEEN '%s' AND '%s' and organization='%s' ",
                    dateFormat.format(startDate), dateFormat.format(endDate),"AADC");

            assert conn != null;
            stmt = conn.createStatement();
            stmt.executeUpdate(querymeter);
            querymeter = String.format(
                    "DELETE FROM " + tableMeterStaging + " WHERE process_date BETWEEN '%s' AND '%s' and organization='%s'",
                    dateFormat.format(startDate), dateFormat.format(endDate),"ADDC");
            
            stmt = conn.createStatement();
            stmt.executeUpdate(querymeter);
            stmt.close();
            conn.close();
            logger.info("\u001B[32m" + "Truncate completed successfully" + " \u001B[0m");

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                    logger.info("\u001B[32m" + "Connection closed PostgreSQL" + " \u001B[0m");
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }
    private void unificarZonas(Date startDate) {
        Statement stmt;
        Connection conn=null;
        ResultSet rs=null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(databaseInt, username, password);
            if (conn != null) {
                logger.info("\u001B[32m" + "Connection established" + " \u001B[0m");
            } else {
                logger.info("\u001B[31m" + "Connection failed" + " \u001B[0m");
            }

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

            String query = 
            		  " SELECT STRING_AGG('''' || zone || '''', ',') AS result  FROM ("
            		+ " select zone "
            		+ " from "
            		+ " sla_meter_staging "
            		+ " where "
            		+ " process_date = '"+dateFormat.format(startDate)+"'"
            		+ " group by zone "
            		+ " having count(*) >5000 ) as zones";
             
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            String zones="";
            if (rs.next()) {
            	zones = rs.getString("result");     
            }  
            String update = 
         		   
         		  " update sla_meter_staging "
         		+ " set zone='No Zone'"
         		+ " where "
         		+ " process_date = '"+dateFormat.format(startDate)+"'"
         		+ " and zone not in ("+zones+")";
            stmt.executeUpdate(update);
            
            stmt.close();
            conn.close();
            logger.info("\u001B[32m" + "Unificando Zonas completed successfully" + " \u001B[0m");

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                    logger.info("\u001B[32m" + "Connection closed PostgreSQL" + " \u001B[0m");
                } catch (SQLException e) {
                    e.printStackTrace();
                }

            }
        }
    }

//    @Scheduled(cron = "${spring.datasource.schedule.truncateonlytwo}", zone = "${spring.datasource.timezone}")
//    public void truncateOnly2Days() {
//        Statement stmt;
//        try {
//            Class.forName("org.postgresql.Driver");
//            Connection conn = DriverManager.getConnection(databaseInt, username, password);
//            if (conn != null) {
//                logger.info("\u001B[32m" + "Connection established" + " \u001B[0m");
//            } else {
//                logger.info("\u001B[31m" + "Connection failed" + " \u001B[0m");
//            }
//
//            LocalDate date = LocalDate.now();
//
//            String dmy = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
//
//            String querycommand = String.format("Delete " +
//                    "from " +
//                    "sla_command_staging scs " +
//                    "where Cast(scs.datetime as Date) = Cast('" + dmy + "' as Date) - 2 " +
//                    "and scs.order_name in ('LoadProfile1', 'EnergyProfile', 'WaterProfile')");
//
//            assert conn != null;
//            stmt = conn.createStatement();
//            stmt.executeUpdate(querycommand);
//            logger.info("\u001B[32m" + "Truncate completed successfully" + " \u001B[0m");
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

   // @Scheduled(cron = "0 0 22 * * *", zone = "${spring.datasource.timezone}")
    @Scheduled(cron = "${spring.datasource.schedule.postgre}", zone = "${spring.datasource.timezone}")
    public void readWrite() {
   
    	 
	    LocalDateTime currentDate = LocalDateTime.now();
		LocalDateTime midnight = currentDate.toLocalDate().atStartOfDay().minusDays(1);
		long startTimestamp = midnight.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
		
	 
		truncatedb(new Date(startTimestamp), new Date(startTimestamp));
		
		logger.info("\u001B[32m" + "Load for PostgreSQL dayly AADC" + " \u001B[0m");

		loadDataMeterByDateAndByOrganization(startTimestamp,"aadc");
	    
		logger.info("\u001B[32m" + "Load for PostgreSQL dayly ADDC" + " \u001B[0m");
        loadDataMeterByDateAndByOrganization(startTimestamp,"addc");
        
        
        unificarZonas(new Date(startTimestamp));
      
    }

    public void postgreExecute(long startTimestamp, long endTimestamp) {
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	truncatedb(new Date(startTimestamp), new Date(endTimestamp));
        
      
        logger.info("\u001B[32m" + "StartExtraction -> " + dateFormat.format( new Date(startTimestamp)) + " \u001B[0m");
        logger.info("\u001B[32m" + "EndExtraction   -> " + dateFormat.format(new Date(endTimestamp)) + " \u001B[0m");
        
        long nextDay=startTimestamp;   
  
        while (nextDay<endTimestamp) {
            
        	  
            loadDataMeterByDateAndByOrganization(nextDay, "aadc");
            loadDataMeterByDateAndByOrganization(nextDay, "addc");
           
            unificarZonas(new Date(nextDay));
            nextDay+=(24 * 60 * 60 * 1000);  
           
        }
        
        

    }

    private String truncateString(String input, int maxLength) {
        return (input.length() > maxLength) ? input.substring(0, maxLength) : input;
    }
    
    public void loadDataMeterByDateAndByOrganization(long startTimestamp ,String schemaName  ) {
        
        logger.info("\u001B[32m" + "Load for PostgreSQL" + " \u001B[0m");
        long filterDate=startTimestamp+(24 * 60 * 60 * 1000);       
        LocalDateTime startExtraction = LocalDateTime.ofInstant(Instant.ofEpochMilli(startTimestamp),
                ZoneId.systemDefault());
        LocalDateTime endExtraction = LocalDateTime.ofInstant(Instant.ofEpochMilli(filterDate),
                ZoneId.systemDefault());
       /* LocalDateTime filterDateDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(filterDate),
                ZoneId.systemDefault());*/
        
        Load loadInit = new Load();
        loadInit.setCreatedAt(LocalDateTime.now());
        loadInit.setExtractionStartDate(startExtraction);
        loadInit.setExtractionEndDate(endExtraction);
        loadInit.setStatus("RUNNING");
        loadInit.setDbsource("PostgreSQL: "+schemaName);
        loadRepository.save(loadInit);
      
        try {
        	          	    
                Connection conn2 = DriverManager.getConnection(databaseExtern, username2, password2);     	        
		        Statement statement = conn2.createStatement();
 		        ResultSet resultSet =null;
 		        List<Long[]> idRanges = new ArrayList<>();
 		        Long id_from=0L,id_to=0L;
           	    Long offset=0L,batch_query_size=40000L;
                while (true) {
                	
	                String query = " SELECT id as id_from, LEAD(id,"+batch_query_size+") OVER (ORDER BY id) as id_to " +
	                        " FROM "+schemaName+".equipment " +
	                        " ORDER BY id " +
	                        " OFFSET " + offset +
	                        " LIMIT " + 1;
	               
	                resultSet = statement.executeQuery(query);
	                
			        if (resultSet.next()) {
			        	id_from = resultSet.getLong("id_from");
	                    id_to = resultSet.getLong("id_to");
	                    if (id_to==0) {
	                    	 
	                    	 id_to=999999999999L;
	                    }
	                    offset += batch_query_size; // Increment offset for the next iteration
	                    idRanges.add(new Long[]{id_from, id_to});
	                }  else {
	                 
	                    break;
	                }
			       
	                
	            }
                resultSet.close();
	            statement.close();
	            conn2.close();
	            for (Long[] idRange : idRanges) {	              
	                extracted(startTimestamp, filterDate, loadInit,  schemaName, idRange[0], idRange[1]);
	            }              
	           
                logger.info("\u001B[32m" + "Extraction completed successfully for "+schemaName+" from external database"
                        + " \u001B[0m");

                loadInit.setEndDate(LocalDateTime.now());
                loadInit.setStatus("FINISH_OK");
                loadRepository.save(loadInit);

                logger.info("\u001B[32m" + "Extraction completed successfully PostgreSQL" + " \u001B[0m");
               
         }
          catch (Exception e) {
                            loadInit.setStatus("NOT_OK");
                            loadInit.setErrorMessage(truncateString(e.getMessage(), 255));
                            loadInit.setCreatedAt(LocalDateTime.now());
                            loadRepository.save(loadInit);
                            e.printStackTrace();
                        }      		
                       
            }

	private void extracted(Long startTimestamp, Long filterDate,Load loadInit,   String schemaName,	Long id_from, Long id_to) throws SQLException {
	
		SimpleDateFormat dateFormatTimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat dateFormatDate = new SimpleDateFormat("yyyy-MM-dd");
			
		logger.info("\u001B[31m"+"Load Data Postgres  ids:"+id_from+" "+id_to+"\u001B[0m");
		logger.info("\u001B[31m"+"Dia"+dateFormatTimeStamp.format(new Date(startTimestamp))+"\u001B[0m");
		logger.info("\u001B[32m" +"Org:    " + schemaName +"  \u001B[0m");
			
		Connection conn2 = DriverManager.getConnection(databaseExtern, username2, password2);;
        
		
		
		Statement stmt2;
		ResultSet rs2;
		String query2 =     " select * " +
			                " from ( " +
			                " SELECT      " +
			                "   e.id as equipment , " +
			                "   COALESCE(mp.priority, 3) as priority, " +
			                "   e.name serial_number, " +
			                "   mpd.city AS zone, " +
			                "   '"+dateFormatDate.format(new Date(startTimestamp))+"'  AS process_date, " +
			                "    ut.type meter_type, " +
			                "   es.code AS status,  " +
			                "   ROW_NUMBER() OVER (PARTITION BY e.serial_number ORDER BY heqs.register_date DESC) AS row_num " +
			                " from " + schemaName + ".equipment e  " +
			                " inner join  " + schemaName + ".equipment_point_configuration epc on epc.equipment  =e.id and epc.end_date is null             " +
			                " inner join  " + schemaName + ".measuring_point mp on epc.measuring_point = mp.id  " +
							" inner join  " + schemaName + ".measuring_point_billing_data  bd on bd.measuring_point =mp.id and  bd.tariff_structure_start_date <='"+dateFormatTimeStamp.format(new Date(filterDate))+"' " +
			                " inner join  " + schemaName + ".historical_equipment_status heqs on e.id = heqs.equipment and  " +
			                "         heqs.register_date <='"+dateFormatTimeStamp.format(new Date(filterDate))+"'    " +
			                " inner join " + schemaName + ".equipment_status es on heqs.status =es.id   " +
			                " left outer join  " + schemaName + ".measuring_point_location_data mpd on mpd.id = mp.location_data             " +
			                " left outer join  " + schemaName + ".utility_type ut on ut.id = mp.utility_type    " +
			                "   where e.id >= "+id_from+" and e.id<"+id_to+"   "+
			                ") reg  " +
			                "where row_num =1;";
				
		
		stmt2 = conn2.createStatement();
		logger.info("Connection established" + query2);
		rs2 = stmt2.executeQuery(query2);
		
		int count = 0;
		int batchSize=1000;//para insertar en lotes
		List<MeterStaging> meterStagingList = new ArrayList<>();
         
		List<String> insertList=new ArrayList<>();
     	
		while (rs2.next()) {
		   
		        String zone = "";
		        if (rs2.getString("zone") == null) {
		            zone = "No Zone";
		        } else {
		            zone = rs2.getString("zone");
		            zone = truncateString(zone.replace("'", "''"), 255);
		        }
			    MeterStaging meterStaging = MeterStaging.builder()
		        		.serial_number(rs2.getString("serial_number"))                		
		        		.meter_type(rs2.getString("meter_type"))
		        		.status(rs2.getString("status"))
		        		.zone(zone)
		        		.priority("P" +rs2.getString("priority"))
		        		.process_date(new Date(startTimestamp))
		        		.load_id(loadInit.getId())
		        		.organization(schemaName.toUpperCase())
		        		.build();
		        meterStagingList.add(meterStaging);
			    count++;
		        if (count % batchSize == 0) {  	
		           	 //stmt1.executeUpdate(generateInsertStatement(meterStagingList ));	
		           	 insertList.add(generateInsertStatement(meterStagingList ));
		        	 meterStagingList.clear();        
		        }
		 }
	   if (meterStagingList.size()>0){
	         //stmt1.executeUpdate(generateInsertStatement(meterStagingList ));
	         insertList.add(generateInsertStatement(meterStagingList ));
	         meterStagingList.clear();
	   }
	   stmt2.close();
	   conn2.close();
	   rs2.close();
	   Connection conn1 = DriverManager.getConnection(databaseInt, username, password);
	   Statement stmt1 = conn1.createStatement();
	   for (String insert:insertList) {
		   stmt1.executeUpdate(insert);
	   }
	   stmt1.close();
	   conn1.close();
	   
		
	}

    private String generateInsertStatement( List<MeterStaging> meterStagingList ) {
        StringBuilder insertStatement = new StringBuilder();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        insertStatement.append(String.format("insert into %s ( Priority, Serial_Number, Zone, Process_date, Organization, Status, Meter_type, load_id) values", tableMeterStaging));
        
        int size = meterStagingList.size();
        for (int i = 0; i < size; i++  ) {
        	MeterStaging recorre=meterStagingList.get(i);
        
            insertStatement.append(String.format("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", 
                                    recorre.getPriority(),
                                    recorre.getSerial_number(),
                                    recorre.getZone(),
                                    dateFormat.format(recorre.getProcess_date()),
                                    recorre.getOrganization(),
                                    recorre.getStatus(),
                                    recorre.getMeter_type(),
                                    recorre.getLoad_id()));
            if (i < size - 1) {
                insertStatement.append(",");
            } else {
                insertStatement.append(";");
            }
            
        }

        return insertStatement.toString();
    }    
}

package com.start.load.application.execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.start.load.entity.CommandStaging;
import com.start.load.entity.Load;
import com.start.load.repository.LoadRepository;

/**
 * @author Roger Manzo
 * @apiNote Service Map
 */
@Configuration
public class ScheduleServiceMongo {

    @Value("${spring.datasource.mongodb}")
    private String mongodb;
    @Value("${spring.datasource.url}")
    private String databaseInt;
    @Value("${spring.datasource.username}")
    private String username;
    @Value("${spring.datasource.password}")
    private String password;
    @Value("${spring.datasource.substring}")
    private Integer days;

    @Value("oum_aadc")
    private String getDatabaseAADC;

    @Value("oum_addc")
    private String getDatabaseADDC;

    @Value("${spring.datasource.timezone}")
    private String timezone;

   

    @Value("${spring.datasource.tables.tablecommandstaging}")
    private String tableCommandStaging;

    private static final Logger logger = LogManager.getLogger(ScheduleServiceMongo.class);
    
    @Autowired
    private LoadRepository loadRepository;
  
    @Scheduled(cron = "${spring.datasource.schedule.mongodb}", zone = "${spring.datasource.timezone}")
    public void mongodb()  {
    	
    	 logger.info("\u001B[32m" + "Load for MongoDB" + " \u001B[0m");
    	 
    	 truncateTable(startDate(-1), endDate(-1),"AADC");
    	 truncateTable(startDate(-1), endDate(-1),"ADDC");
		 loadDataByRangeDate(startDate(-1),endDate(-1));
		 
    }
    
    
    public void loadDataByRangeDate(long paramInitDate,long paramEndDate)  {

        logger.info("\u001B[32m" + "Load for MongoDB" + " \u001B[0m");

        MongoClient mongoClient = null;
       
        Connection conn = null;

        Date dateDubai = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        df.setTimeZone(TimeZone.getTimeZone("Asia/Dubai"));

        logger.info("\u001B[32m" + "DATE DUBAI: " + df.format(dateDubai) + " \u001B[0m");

        try {
			Class.forName("org.postgresql.Driver");
		
			conn = DriverManager.getConnection(databaseInt, username, password);
		
        } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


            String uri = mongodb;
            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(uri))
                    .build();

            mongoClient = MongoClients.create(settings);
            MongoDatabase databaseAADC = mongoClient.getDatabase(getDatabaseAADC);
            MongoDatabase databaseADDC = mongoClient.getDatabase(getDatabaseADDC);
            MongoCollection<Document> mongoCollectionAADC = databaseAADC.getCollection("call");
            MongoCollection<Document> mongoCollectionADDC = databaseADDC.getCollection("call");
          
           
            Load loadInit = new Load();
            loadInit.setCreatedAt(LocalDateTime.now());
            LocalDateTime startExtraction = LocalDateTime.ofInstant(Instant.ofEpochMilli(paramInitDate),
                    ZoneId.systemDefault());
            LocalDateTime endExtraction = LocalDateTime.ofInstant(Instant.ofEpochMilli(paramInitDate),
                    ZoneId.systemDefault());
            logger.info("\u001B[32m" + "StartExtraction -> " + startExtraction + " \u001B[0m");
            logger.info("\u001B[32m" + "EndExtraction -> " + endExtraction + " \u001B[0m");
            loadInit.setExtractionStartDate(startExtraction);
            loadInit.setExtractionEndDate(endExtraction);
            loadInit.setStatus("RUNNING");
            loadInit.setDbsource("MongoDB");
            loadRepository.save(loadInit);

            
           
            Statement stmt1;
			try {
				    truncateTable(paramInitDate,paramEndDate, "AADC");
		  		    stmt1 = conn.createStatement();
				    loadComandByDateAndOrganization(stmt1, mongoCollectionAADC,"AADC", loadInit.getId(),paramInitDate,paramEndDate);
				    truncateTable(paramInitDate,paramEndDate, "ADDC");
		          
		            loadComandByDateAndOrganization(stmt1, mongoCollectionADDC,"ADDC", loadInit.getId(),paramInitDate,paramEndDate);
		            loadInit.setStatus("FINISH_OK");
		            loadInit.setEndDate(LocalDateTime.now());
		            loadRepository.save(loadInit);
		            stmt1.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				loadInit.setStatus("NOT_OK");
                loadInit.setErrorMessage(truncateString(e.getMessage(), 255));
                loadInit.setCreatedAt(LocalDateTime.now());
                loadRepository.save(loadInit);
                e.printStackTrace();
			}
            
            

           
            
    }

    private void loadComandByDateAndOrganization(Statement stmt1, MongoCollection<Document> mongoCollection,String organization, int loadId,long initDate,long endDate) throws SQLException {
		
    	long currentTime = initDate;
    	
    	while (currentTime < endDate) {
         	
    		loadComandByDateAndOrganizationSplit(stmt1, mongoCollection, organization,loadId, currentTime, currentTime + 3600000);  
           
            currentTime += 3600000;
        }
    }

	private void loadComandByDateAndOrganizationSplit(Statement stmt1, MongoCollection<Document> mongoCollection,String organization, int loadId,long initDate,long endDate) throws SQLException {
		     SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	        
		
		     logger.info("\u001B[32m" + "loadComandByDateAndOrganizationSplit->  " + initDate+" "+endDate+" \u001B[0m");
		     logger.info("\u001B[32m" + "Start:  " + dateFormat.format(new Date(initDate)) +"  \u001B[0m");
		     logger.info("\u001B[32m" + "End:    " + dateFormat.format(new Date(endDate)) +"  \u001B[0m");
		     logger.info("\u001B[32m" + "Org:    " + organization +"  \u001B[0m");
        
		    Bson filterStartDate = Filters.gte("datetime", initDate);
		    Bson filterEndDate = Filters.lte("datetime", endDate);
		    Bson filtercm = Filters.ne("tasks.order.name", "COMMISSION");
		    Bson filterhb = Filters.ne("tasks.order.name", "HearBeat");
		    Bson filtercmm = Filters.ne("tasks.order.name", "Commission");
		    Bson filterge = Filters.ne("tasks.order.name", "GatewayEventsAndAlarms");
		    Bson filternm = Filters.and(Filters.exists("tasks.order.name", true),
		            Filters.ne("tasks.order.name", null));
		    Bson filterdt = Filters.and(Filters.exists("tasks.order.initTime", true),
		            Filters.ne("tasks.order.initTime", null));
		    Bson filteret = Filters.and(Filters.exists("tasks.order.finishTime", true),
		            Filters.ne("tasks.order.finishTime", null));

		     
		    Bson finalFilter = Filters.and(filterStartDate, filterEndDate, filterhb,
		            filtercmm, filterge, filteret, filterdt, filtercm, filternm);

		    // Convert the filter to a Document and then to its JSON string representation
		    String jsonString = finalFilter.toBsonDocument(Document.class, mongoCollection.getCodecRegistry()).toJson();

		    MongoCursor<Document> cursor = mongoCollection.find(finalFilter).cursor();
		    int count = 0;
		    int batchSize=1000;
		   
		    
		    List<CommandStaging> commandsStagingList= new ArrayList<>();
		   
		    if (cursor.hasNext()) {

		            while (cursor.hasNext()) {
		            	count++;
		                Document next = cursor.next();
		                JSONObject jsonObject = new JSONObject(next);
		                String device = "";
		                Long datetime = jsonObject.getLong("datetime");
		                JSONArray tasks = jsonObject.getJSONArray("tasks");
		                if (tasks.getJSONObject(0).has("deviceName")) {
		                	device = tasks.getJSONObject(0).getString("deviceName");
		                }
		                JSONArray order = tasks.getJSONObject(0).getJSONArray("order");
		                String name = order.getJSONObject(0).getString("name");
		                Long initTime = order.getJSONObject(0).getLong("initTime");                          
		                Long finishTime = order.getJSONObject(0).getLong("finishTime");
		                String status = "";
		                if (order.getJSONObject(0).has("status")) {
		                    status = order.getJSONObject(0).getString("status");
		                } else {
		                }                      
		                CommandStaging commandStaging = CommandStaging.builder()
		                	    .device_name(device)
		                	    .order_name(name)
		                	    .order_status(status)
		                	    .dateTimeLong(datetime) // Assuming outDate is of type Date
		                	    .organization(organization)
		                	    .initTimeLong(initTime) // Assuming itTime is of type Date
		                	    .finishTimeLong(finishTime) // Assuming fTime is of type Date
		                	    .load_id(loadId) // Assuming loadInit.getId() returns an Integer
		                	    .build();
		                commandsStagingList.add(commandStaging);
		                
		                if (count % batchSize == 0) {  	
		   		         	 
							 stmt1.executeUpdate(generateInsertStatement(commandsStagingList ));
							
		   		             commandsStagingList.clear();        
		                }                       
		                
		            }
		            if (commandsStagingList.size()>0) {
		            	stmt1.executeUpdate(generateInsertStatement(commandsStagingList ));
		            
		            }
		          

		        } 
	}           

                    
                    
    

    private String generateInsertStatement(List<CommandStaging> commandStagingList) {
    	  StringBuilder insertStatement = new StringBuilder();
    	 
    	  
    	  insertStatement.append(String.format("insert into %s ( device_name, order_name, order_status, datetime, organization, inittime, finishtime, load_id) values ", tableCommandStaging));
       
             
          int size = commandStagingList.size();
          for (int i = 0; i < size; i++  ) {
        	  CommandStaging recorre=commandStagingList.get(i);
          
              insertStatement.append(String.format("('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')", 
                                      recorre.getDevice_name(),
                                      recorre.getOrder_name(),
                                      recorre.getOrder_status(),
                                      convertDate(recorre.getDateTimeLong()),
                                      recorre.getOrganization(),
                                      convertDate(recorre.getInitTimeLong()),
                                      convertDate(recorre.getFinishTimeLong()),
                                      recorre.getLoad_id()));
              if (i < size - 1) {
                  insertStatement.append(",");
              } else {
                  insertStatement.append(";");
              }
              
          }

          return insertStatement.toString();
      
    }
    	

    @Scheduled(cron = "${spring.datasource.schedule.mongodbtwo}", zone = "${spring.datasource.timezone}")
    public void mongodbOnly2Days() throws SQLException {
    	Long startDateMinus2=startDate(-2);
    	Long endDateMinus2=endDate(-2);
    	Long startDateMinus1=startDate(-1);
    	Long endDateMinus1=endDate(-1);
    	
    	logger.info("\u001B[32m" + "Load for MongoDB2days" + " \u001B[0m");
        truncateTable(startDateMinus2, endDateMinus2,"AADC");
        truncateTable(startDateMinus2, endDateMinus2,"ADDC");
        loadDataByRangeDate(startDateMinus2,endDateMinus2);  	 
        
        
        truncateTable(startDateMinus1, endDateMinus1,"AADC");
        truncateTable(startDateMinus1, endDateMinus1,"ADDC");
        loadDataByRangeDate(startDateMinus1,endDateMinus1);  	  	 
    }

    public Map<String, String> mongodbExecute(long startTimestamp, long endTimestamp)  {

        logger.info("\u001B[32m" + "Load for MongoDB" + " \u001B[0m");

         

        truncateTable(startTimestamp, endTimestamp,"AADC");
        truncateTable(startTimestamp, endTimestamp,"ADDC");
    
        loadDataByRangeDate(startTimestamp,endTimestamp);  	 
      
        Map<String, String> mapped = new HashMap<>();
        mapped.put("extraction", "ok");
        mapped.put("start time", String.valueOf(startTimestamp));
        mapped.put("end time", String.valueOf(endTimestamp));
        return mapped;
    }

    
    public  long startDate(int days) {
        Instant nowUtc = Instant.now();
        ZoneId asiaDubai = ZoneId.of("Asia/Dubai");
        ZonedDateTime nowAsiaDubai = ZonedDateTime.ofInstant(nowUtc, asiaDubai);
        Calendar c = Calendar.getInstance();
        c.set(nowAsiaDubai.getYear(), nowAsiaDubai.getMonthValue() - 1, nowAsiaDubai.getDayOfMonth(),
                nowAsiaDubai.getHour(), nowAsiaDubai.getMinute());
        c.add(Calendar.DATE, days);
        Date date = c.getTime();
        SimpleDateFormat startFormat = new SimpleDateFormat("yyyy-MM-dd'T'00:00:00.000'Z'");
        String start = startFormat.format(date);
        Instant instant1 = Instant.parse(start);
        return instant1.getEpochSecond() * 1000;
    }

    public  long endDate(int days) {
        Instant nowUtc = Instant.now();
        ZoneId asiaDubai = ZoneId.of("Asia/Dubai");
        ZonedDateTime nowAsiaDubai = ZonedDateTime.ofInstant(nowUtc, asiaDubai);
        Calendar c = Calendar.getInstance();
        c.set(nowAsiaDubai.getYear(), nowAsiaDubai.getMonthValue() - 1, nowAsiaDubai.getDayOfMonth(),
                nowAsiaDubai.getHour(), nowAsiaDubai.getMinute());
        c.add(Calendar.DATE, days);
        Date date = c.getTime();
        SimpleDateFormat startFormat = new SimpleDateFormat("yyyy-MM-dd'T'23:59:59.999'Z'");
        String start = startFormat.format(date);
        Instant instant1 = Instant.parse(start);
        return instant1.getEpochSecond() * 1000;
    }

    public  String convertDate(Long received) {
        String epochString = String.valueOf(received);
        long epoch = Long.parseLong(epochString);
        Instant nowUtc = Instant.ofEpochMilli(epoch);
        ZoneId asiaDubai = ZoneId.of("UTC+00");
        ZonedDateTime nowAsiaDubai = ZonedDateTime.ofInstant(nowUtc, asiaDubai);
        Calendar c = Calendar.getInstance();

        c.set(nowAsiaDubai.getYear(), nowAsiaDubai.getMonthValue() - 1, nowAsiaDubai.getDayOfMonth(),
                nowAsiaDubai.getHour(), nowAsiaDubai.getMinute(), nowAsiaDubai.getSecond());
        Date d = c.getTime();
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String outDate = sdfDate.format(d);
        return outDate;
    }
    
    private void truncateTable(long startTimestamp, long endTimestamp,String organization) {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(databaseInt, username, password);
            if (conn != null) {
                logger.info("\u001B[32m" + "Connection established" + " \u001B[0m");
            } else {
                logger.info("\u001B[31m" + "Connection failed" + " \u001B[0m");
            }

            String truncateCommand = "DELETE FROM " + tableCommandStaging + " WHERE datetime BETWEEN ? AND ? and organization =?";
            try (PreparedStatement pstmt = conn.prepareStatement(truncateCommand)) {
                pstmt.setTimestamp(1, new Timestamp(startTimestamp));
                pstmt.setTimestamp(2, new Timestamp(endTimestamp));
                pstmt.setString(3, organization);
             
                pstmt.executeUpdate();
                logger.info("\u001B[32m" + "Table CommandStaging successfully Truncate" + " \u001B[0m");
            } catch (SQLException e) {
                logger.error("\u001B[31m" + "Error truncating table: " + e.getMessage() + "\u001B[0m");
            }

        } catch (SQLException e) {
            logger.error("\u001B[31m" + "Error truncating table: " + e.getMessage() + "\u001B[0m");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
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
    private String truncateString(String input, int maxLength) {
        return (input.length() > maxLength) ? input.substring(0, maxLength) : input;
    }
    
}
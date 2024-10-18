package com.start.load.controller.action;

import com.start.load.application.execution.ScheduleServiceMongo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.Map;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*", maxAge = 3600)
@RequestMapping("/load")
public class MongoAction {

    @Autowired
    private ScheduleServiceMongo scheduleServiceMongo;

    //@Async("asyncExecutor")
    @PostMapping("/actions/mongodb")
    public ResponseEntity<?> executeMongoDB(
            @RequestParam("startDate") @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
            @RequestParam("endDate") @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate) {
        long startTimestamp = startDate.toInstant().toEpochMilli();
        long endTimestamp = endDate.toInstant().toEpochMilli();
        Map<String, String> result = scheduleServiceMongo.mongodbExecute(startTimestamp, endTimestamp);

        if (result.containsKey("extraction") && result.get("extraction").equals("ok")) {
            return ResponseEntity.status(HttpStatus.OK).body("Extraction completed successfully");
        } else {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Extraction failed");
        }
    }

}

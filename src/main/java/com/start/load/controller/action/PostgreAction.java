package com.start.load.controller.action;

import com.start.load.application.execution.ScheduleServicePostgre;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*", maxAge = 3600)
@RequestMapping("/load")
public class PostgreAction {

    @Autowired
    private ScheduleServicePostgre scheduleServicePostgre;

    //@Async("asyncExecutor")
    @PostMapping("/actions/postgre")
    public ResponseEntity<?> executePostgreSQL(@RequestParam("startDate") @DateTimeFormat(pattern = "yyyy-MM-dd") Date startDate,
                                            @RequestParam("endDate") @DateTimeFormat(pattern = "yyyy-MM-dd") Date endDate){

        long startTimestamp = startDate.toInstant().toEpochMilli();
        long endTimestamp = endDate.toInstant().toEpochMilli();

       
        scheduleServicePostgre.postgreExecute(startTimestamp, endTimestamp);

        return ResponseEntity.status(HttpStatus.OK).body("Extraction completed successfully");
    }

}

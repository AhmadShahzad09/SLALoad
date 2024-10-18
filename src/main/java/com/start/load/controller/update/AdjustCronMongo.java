package com.start.load.controller.update;

import com.start.load.application.restart.SetScheduleMongo;
import com.start.load.dto.CronExpressionDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*", maxAge = 3600)
@RequestMapping("/load")
public class AdjustCronMongo {

    @Autowired
    private SetScheduleMongo setScheduleMongo;

    @PostMapping("/restart/mongodb")
    public ResponseEntity<?> updateCronExpression(@RequestBody CronExpressionDTO cronExpressionDTO) {
        try {
            String newCronExpression = cronExpressionDTO.getCronExpression();
            setScheduleMongo.updateCronExpression(newCronExpression);
            return ResponseEntity.ok().build();
        }catch (Exception ex){
            System.out.println(ex.fillInStackTrace());
            System.out.println(ex.getMessage());
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping("/get-cron-mongodb")
    public ResponseEntity<String> getCronExpression() {
        String currentCronExpression = setScheduleMongo.getCronExpression();
        return ResponseEntity.ok(currentCronExpression);
    }

}

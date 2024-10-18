package com.start.load.controller.load;

import com.start.load.application.execution.ScheduleServicePostgre;
import com.start.load.entity.Load;
import com.start.load.services.LoadServiceImpl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*", maxAge = 3600)
@RequestMapping("/load")
public class LoadController {

    @Autowired
    private LoadServiceImpl loadService;

    private static final Logger logger = LogManager.getLogger(ScheduleServicePostgre.class);


    @GetMapping
    public ResponseEntity<Page<Load>> getAllLoads(Pageable pageable) {
        Page<Load> loads = loadService.getAllLoads(pageable);
        return ResponseEntity.ok(loads);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Load> getLoadById(@PathVariable int id) {
        Load load = loadService.getLoadById(id);
        return ResponseEntity.ok(load);
    }

    @PostMapping
    public ResponseEntity<Void> saveLoad(@RequestBody Load load) {
        loadService.saveLoad(load);
        return ResponseEntity.ok().build();
    }

    @PutMapping("/{id}")
    public ResponseEntity<Void> updateLoad(@PathVariable int id, @RequestBody Load load) {
        load.setId(id);
        loadService.updateLoad(load);
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteLoad(@PathVariable int id) {
        loadService.deleteLoad(id);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/status/{status}")
    public ResponseEntity<Page<Load>> getLoadsByStatus(@PathVariable String status, Pageable pageable) {
        Page<Load> loads = loadService.getLoadsByStatus(status, pageable);
        return ResponseEntity.ok(loads);
    }

    @GetMapping("/search")
    public ResponseEntity<Page<Load>> searchLoadsByErrorMessage(@RequestParam String keyword, Pageable pageable) {
        Page<Load> loads = loadService.searchLoadsByErrorMessage(keyword, pageable);
        return ResponseEntity.ok(loads);
    }

    @GetMapping("/date-range")
    public ResponseEntity<Page<Load>> getLoadsInDateRange(
            @RequestParam String startDate, @RequestParam String endDate, Pageable pageable) {
        LocalDateTime startDateTime = LocalDateTime.parse(startDate);
        LocalDateTime endDateTime = LocalDateTime.parse(endDate);
        Page<Load> loads = loadService.getLoadsInDateRange(startDateTime, endDateTime, pageable);
        return ResponseEntity.ok(loads);
    }

    @GetMapping("/custom-filters")
    public ResponseEntity<Page<Load>> findByCustomFilters(
            @RequestParam(required = false) String dbsource,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String startdt,
            @RequestParam(required = false) String enddt,
            @PageableDefault(sort = "extraction_start_date", direction = Sort.Direction.DESC) Pageable pageable) {

        logger.info("\u001B[32m" +"dbsource:"+dbsource+" \u001B[0m");
        logger.info("\u001B[32m" +"status:"+status+" \u001B[0m");
        logger.info("\u001B[32m" +"startdt:"+startdt+" \u001B[0m");
        logger.info("\u001B[32m" +"enddt:"+enddt+" \u001B[0m");

        if (enddt == null || enddt.isEmpty()) {
            Page<Load> loads = loadService.findByCustomFiltersWithStartDtOnly(dbsource, status, startdt, pageable);
            return ResponseEntity.ok(loads);
        }

        if (startdt == null || startdt.isEmpty()) {
            Page<Load> loads = loadService.findByCustomFiltersWithEndDtOnly(dbsource, status, enddt, pageable);
            return ResponseEntity.ok(loads);
        }

        Page<Load> loads = loadService.findByCustomFilters(dbsource, status, startdt, enddt, pageable);
        return ResponseEntity.ok(loads);
    }

}

package com.start.load.controller.logger;

import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;

@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*", maxAge = 3600)
@RequestMapping("/load")
public class MongoLogger {

    private static final String LOG_FILE_PATH = "logs/spring.log";

    @GetMapping("/logs/mongodb")
    public ResponseEntity<Resource> getSpringLogs(@RequestParam(name = "action", defaultValue = "download") String action) throws IOException {
        Resource resource = loadLogFileAsResource();

        if (action.equals("view")) {
            return ResponseEntity.ok()
                    .contentLength(resource.contentLength())
                    .contentType(MediaType.TEXT_PLAIN)
                    .body(resource);
        } else {
            return ResponseEntity.ok()
                    .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + resource.getFilename() + "\"")
                    .body(resource);
        }
    }

    private Resource loadLogFileAsResource() throws MalformedURLException {
        Path filePath = Paths.get(LOG_FILE_PATH).toAbsolutePath();
        Resource resource = new UrlResource(filePath.toUri());

        if (resource.exists()) {
            return resource;
        } else {
            throw new RuntimeException("Archivo de registro no encontrado: " + LOG_FILE_PATH);
        }
    }
}

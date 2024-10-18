package com.start.load.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.springframework.format.annotation.DateTimeFormat;
import javax.persistence.*;
import java.time.LocalDateTime;

@Entity
@NoArgsConstructor @AllArgsConstructor
@Getter @Setter
@Table(name = "sla_load")
public class Load {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(nullable = true)
    @DateTimeFormat(pattern = "yyyy-MM-dd hh:mm:ss")
    @CreationTimestamp
    private LocalDateTime createdAt;

    @Column(nullable = true)
    private String status;

    @Column(nullable = true)
    private String errorMessage;

    @Column(nullable = true)
    private String dbsource;

    @Column(nullable = true)
    @DateTimeFormat(pattern = "yyyy-MM-dd hh:mm:ss")
    private LocalDateTime extractionStartDate;

    @Column(nullable = true)
    @DateTimeFormat(pattern = "yyyy-MM-dd hh:mm:ss")
    private LocalDateTime extractionEndDate;

    @Column(nullable = true)
    @DateTimeFormat(pattern = "yyyy-MM-dd hh:mm:ss")
    private LocalDateTime endDate;

}

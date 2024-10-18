package com.start.load.entity;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Entity
@Getter 
@Setter
@Table(name = "sla_meter_staging", schema = "public")
public class MeterStaging {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;
    private String priority;
    private String serial_number;
    private String zone;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date process_date;
    private String organization;
    private String status;
    private String meter_type;
    private Integer load_id;
    private String meter_model;
}

package com.start.load.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;
import javax.persistence.*;
import java.util.Date;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Builder
@Entity
@Table(name = "SLA_COMMAND_STAGING", schema = "public")
public class CommandStaging {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private long id;

    private String device_name;
    private String order_name;
    private String order_status;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date datetime;
    private String organization;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date inittime;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private Date finishtime;
    private Integer load_id;

    @Transient
    private Long dateTimeLong;
    @Transient
    private Long initTimeLong;
    @Transient
    private Long finishTimeLong;
    
}

package com.start.load.application.restart;

import com.start.load.application.execution.ScheduleServiceMongo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.concurrent.ScheduledFuture;

@Service
public class SetScheduleMongo {

    @Value("${spring.datasource.schedule.mongodb}")
    private String cronExpression;

    private final TaskScheduler taskScheduler;
    private ScheduledFuture<?> scheduledFuture;

    @Autowired
    private ScheduleServiceMongo scheduleServiceMongo;

    public SetScheduleMongo(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    @PostConstruct
    private void init() {
        if (StringUtils.isEmpty(cronExpression)) {
            throw new IllegalArgumentException("La expresión cron no puede estar vacía");
        }
        this.scheduledFuture = initializeScheduledTask();
    }

    private ScheduledFuture<?> initializeScheduledTask() {
        return taskScheduler.schedule(this::executeScheduledTask, new CronTrigger(cronExpression));
    }

    public void updateCronExpression(String newCronExpression) {
        this.cronExpression = newCronExpression;
        this.scheduledFuture.cancel(false);
        this.scheduledFuture = initializeScheduledTask();
    }

    public String getCronExpression() {
        return cronExpression;
    }

    private void executeScheduledTask() {
        scheduleServiceMongo.mongodb();
    }

}

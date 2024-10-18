package com.start.load.repository;

import com.start.load.entity.Load;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface LoadRepository extends JpaRepository<Load, Integer> {

    List<Load> findByStatus(String status);

    List<Load> findByErrorMessageContaining(String keyword);

    List<Load> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate);

    Page<Load> findByStatus(String status, Pageable pageable);

    Page<Load> findByErrorMessageContaining(String keyword, Pageable pageable);

    Page<Load> findByCreatedAtBetween(LocalDateTime startDate, LocalDateTime endDate, Pageable pageable);

    @Query(value = "SELECT * FROM sla_load s " +
            "WHERE true=true " +
            "AND (:dbsource IS NULL OR :dbsource = '' OR s.dbsource = CAST(:dbsource AS character varying)) " +
            "AND (:status IS NULL OR :status = '' OR s.status = CAST(:status AS character varying)) " +
            "AND (" +
            ":startdt IS NULL " +
            "OR :startdt = '' " +
            "OR CAST(s.created_at AS Date) >= CAST(CAST(:startdt AS character varying) AS Date)" +
            ") AND (" +
            ":startdt IS NULL " +
            "OR :startdt = '' " +
            "OR CAST(s.created_at AS Date) <= CAST(CAST(:enddt AS character varying) AS Date))", nativeQuery = true)
    Page<Load> findByCustomFilters(@Param("dbsource") String dbsource,
                                   @Param("status") String status,
                                   @Param("startdt") String startdt,
                                   @Param("enddt") String enddt,
                                   Pageable pageable);

    @Query(value = "SELECT * FROM sla_load s " +
            "WHERE true=true " +
            "AND (:dbsource IS NULL OR :dbsource = '' OR s.dbsource = CAST(:dbsource AS character varying)) " +
            "AND (:status IS NULL OR :status = '' OR s.status = CAST(:status AS character varying)) " +
            "AND (" +
            ":startdt IS NULL " +
            "OR :startdt = '' " +
            "OR CAST(s.created_at AS Date) >= CAST(CAST(:startdt AS character varying) AS Date)" +
            ")", nativeQuery = true)
    Page<Load> findByCustomFiltersWithStartDtOnly(@Param("dbsource") String dbsource,
                                                  @Param("status") String status,
                                                  @Param("startdt") String startdt,
                                                  Pageable pageable);


    @Query(value = "SELECT * FROM sla_load s " +
            "WHERE true=true " +
            "AND (:dbsource IS NULL OR :dbsource = '' OR s.dbsource = CAST(:dbsource AS character varying)) " +
            "AND (:status IS NULL OR :status = '' OR s.status = CAST(:status AS character varying)) " +
            "AND (" +
            ":enddt IS NULL " +
            "OR :enddt = '' " +
            "OR CAST(s.created_at AS Date) <= CAST(CAST(:enddt AS character varying) AS Date)" +
            ")", nativeQuery = true)
    Page<Load> findByCustomFiltersWithEndDtOnly(@Param("dbsource") String dbsource,
                                                @Param("status") String status,
                                                @Param("enddt") String enddt,
                                                Pageable pageable);
}

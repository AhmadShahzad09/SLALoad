package com.start.load.services;

import com.start.load.entity.Load;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;

public interface LoadService {

        List<Load> getAllLoads();

        Load getLoadById(int id);

        void saveLoad(Load load);

        void updateLoad(Load load);

        void deleteLoad(int id);

        List<Load> getLoadsByStatus(String status);

        List<Load> searchLoadsByErrorMessage(String keyword);

        List<Load> getLoadsInDateRange(LocalDateTime startDate, LocalDateTime endDate);

        Page<Load> getAllLoads(Pageable pageable);

        Page<Load> getLoadsByStatus(String status, Pageable pageable);

        Page<Load> searchLoadsByErrorMessage(String keyword, Pageable pageable);

        Page<Load> getLoadsInDateRange(LocalDateTime startDate, LocalDateTime endDate, Pageable pageable);

        Page<Load> findByCustomFilters(String dbsource, String status, String startdt, String enddt, Pageable pageable);

        Page<Load> findByCustomFiltersWithStartDtOnly(String dbsource, String status, String startdt,
                        Pageable pageable);

        Page<Load> findByCustomFiltersWithEndDtOnly(String dbsource, String status, String enddt, Pageable pageable);

}

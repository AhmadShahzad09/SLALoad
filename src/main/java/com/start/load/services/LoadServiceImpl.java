package com.start.load.services;

import com.start.load.entity.Load;
import com.start.load.repository.LoadRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class LoadServiceImpl implements LoadService {

    @Autowired
    private LoadRepository loadRepository;

    @Override
    public List<Load> getAllLoads() {
        return loadRepository.findAll();
    }

    @Override
    public Load getLoadById(int id) {
        return loadRepository.findById(id).orElse(null);
    }

    @Override
    public void saveLoad(Load load) {
        loadRepository.save(load);
    }

    @Override
    public void updateLoad(Load load) {
        if (loadRepository.existsById(load.getId())) {
            loadRepository.save(load);
        }
    }

    @Override
    public void deleteLoad(int id) {
        loadRepository.deleteById(id);
    }

    @Override
    public List<Load> getLoadsByStatus(String status) {
        return loadRepository.findByStatus(status);
    }

    @Override
    public List<Load> searchLoadsByErrorMessage(String keyword) {
        return loadRepository.findByErrorMessageContaining(keyword);
    }

    @Override
    public List<Load> getLoadsInDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        return loadRepository.findByCreatedAtBetween(startDate, endDate);
    }

    @Override
    public Page<Load> getAllLoads(Pageable pageable) {
        return loadRepository.findAll(pageable);
    }

    @Override
    public Page<Load> getLoadsByStatus(String status, Pageable pageable) {
        return loadRepository.findByStatus(status, pageable);
    }

    @Override
    public Page<Load> searchLoadsByErrorMessage(String keyword, Pageable pageable) {
        return loadRepository.findByErrorMessageContaining(keyword, pageable);
    }

    @Override
    public Page<Load> getLoadsInDateRange(LocalDateTime startDate, LocalDateTime endDate, Pageable pageable) {
        return loadRepository.findByCreatedAtBetween(startDate, endDate, pageable);
    }

    @Override
    public Page<Load> findByCustomFilters(String dbsource, String status, String startdt, String enddt,
            Pageable pageable) {
        return loadRepository.findByCustomFilters(dbsource, status, startdt, enddt, pageable);
    }

    @Override
    public Page<Load> findByCustomFiltersWithStartDtOnly(String dbsource, String status, String startdt,
            Pageable pageable) {
        return loadRepository.findByCustomFiltersWithStartDtOnly(dbsource, status, startdt, pageable);
    }

    @Override
    public Page<Load> findByCustomFiltersWithEndDtOnly(String dbsource, String status, String enddt,
            Pageable pageable) {
        return loadRepository.findByCustomFiltersWithEndDtOnly(dbsource, status, enddt, pageable);
    }

}

package org.example.sparkdemo.repository;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SalesDataRepository extends JpaRepository<org.example.sparkdemo.entity.SalesData, Long> {
}

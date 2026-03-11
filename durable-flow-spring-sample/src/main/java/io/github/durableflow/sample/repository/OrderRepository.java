package io.github.durableflow.sample.repository;

import io.github.durableflow.sample.entity.OrderRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Spring Data JPA repository for {@link OrderRecord}.
 */
@Repository
public interface OrderRepository extends JpaRepository<OrderRecord, Long> {

    /**
     * Finds an order by its business-key order ID.
     *
     * @param orderId the unique order identifier
     * @return the matching record, or empty if not found
     */
    Optional<OrderRecord> findByOrderId(String orderId);
}

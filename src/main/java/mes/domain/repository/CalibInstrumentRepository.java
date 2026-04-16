package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.CalibInstrument;

@Repository
public interface CalibInstrumentRepository extends JpaRepository<CalibInstrument, Integer> {

	CalibInstrument getCalibInstrumentById(Integer id);
}

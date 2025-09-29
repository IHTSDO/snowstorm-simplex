package org.snomed.snowstorm.core.data.services;

import org.junit.jupiter.api.Test;
import org.snomed.snowstorm.AbstractTest;
import org.snomed.snowstorm.core.data.domain.CodeSystem;
import org.snomed.snowstorm.core.data.domain.Concept;
import org.snomed.snowstorm.core.data.domain.Concepts;
import org.snomed.snowstorm.core.data.domain.Description;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ConceptChangeReportServiceTest extends AbstractTest {

	@Autowired
	private ConceptChangeReportService conceptChangeReportService;

	@Autowired
	private ConceptService conceptService;

	@Autowired
	private CodeSystemService codeSystemService;

	@Test
	void test() throws ServiceException {

		String path = "MAIN";
		conceptService.create(new Concept(Concepts.CLINICAL_FINDING).addDescription(new Description("Clinical finding")), path);

		// Nothing returned if concept not versioned
		List<Long> changedConcepts = conceptChangeReportService.findChangedConcepts(path, 20250101, Collections.emptySet());
		assertEquals(0, changedConcepts.size());

		// Version concept
		CodeSystem codeSystem = codeSystemService.createCodeSystem(new CodeSystem("SNOMEDCT", path));
		codeSystemService.createVersion(codeSystem, 20250201, "");

		// Concept returned when query date pre-dates concept versioning
		changedConcepts = conceptChangeReportService.findChangedConcepts(path, 20250101, Collections.emptySet());
		assertEquals(1, changedConcepts.size());
		assertEquals(Long.parseLong(Concepts.CLINICAL_FINDING), changedConcepts.get(0));

		// Nothing returned when query date matches version date
		changedConcepts = conceptChangeReportService.findChangedConcepts(path, 20250201, Collections.emptySet());
		assertEquals(0, changedConcepts.size());
	}

}

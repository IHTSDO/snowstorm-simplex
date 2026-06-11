package org.snomed.snowstorm.rest;

import org.junit.jupiter.api.Test;
import org.snomed.snowstorm.TestConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, classes = TestConfig.class)
class SwaggerUiAvailabilityTest {

	@Autowired
	private TestRestTemplate restTemplate;

	@Test
	void swaggerUiIndexIsAvailable() {
		ResponseEntity<String> response = restTemplate.getForEntity("/swagger-ui/index.html", String.class);
		assertEquals(HttpStatus.OK, response.getStatusCode());
		assertTrue(response.getBody() != null && response.getBody().contains("Swagger UI"));
	}
}

package org.snomed.snowstorm.loadtest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snomed.snowstorm.core.data.domain.Concept;
import org.snomed.snowstorm.core.data.domain.ConceptView;
import org.snomed.snowstorm.core.data.domain.Concepts;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Test script to support load testing.
 * This script was written quickly for occasional non-production use.
 *
 * Any number of users can be simulated concurrently. The number of times the users create a concept can be set.
 * NOTE: These are manic turbo-users who are faster than humanly possible!
 *
 * Each time this script is run a load-test branch will be created with all work performed in subbranches.
 *
 * Each user will run the create concept procedure which is:
 * - Create branch
 * - Randomly pick a hierarchy to work in
 * - Run '<{hierarchy id}' ECL search
 * - Pick a random concept from the first page of results
 * - Validate the concept
 * - Clear the component ids (clone)
 * - Prefix the FSN with 'Cloned '
 * - Save the cloned concept
 * - Repeat all
 *
 * Update CONCURRENT_USERS to set the number of users simulated concurrently.
 * Update CONCEPTS_TO_CREATE_PER_USER to set the number times each user will run through the authoring procedure.
 * Update HIERARCHIES_TO_AUTHOR_IN to use more hierarchies.
 */
public class ManualLoadTest {

	// Script configuration variables
	private static final String SNOWSTORM_API_URI = "http://localhost:8080/snowstorm/snomed-ct/v2";
	private static final String COOKIE = "dev-ims-ihtsdo=YOUR_COOKIE_HERE";
	private static final int CONCURRENT_USERS = 1;
	private static final int CONCEPTS_TO_CREATE_PER_USER = 1;
	private static final int USER_START_STAGGER = 1;
	private static final List<String> HIERARCHIES_TO_AUTHOR_IN = Lists.newArrayList(
			Concepts.CLINICAL_FINDING,
			"123037004",// 123037004 |Body structure (body structure)|
			"362958002" // 362958002 |Procedure by site (procedure)|
	);

	// Internal variables
	private static final ParameterizedTypeReference<ItemsPagePojo<ConceptResult>> PAGE_OF_CONCEPTS_TYPE = new ParameterizedTypeReference<ItemsPagePojo<ConceptResult>>() {};
	private static final Logger LOGGER = LoggerFactory.getLogger(ManualLoadTest.class);

	private RestTemplate restTemplate;
	private String loadTestBranch;
	private ObjectMapper objectMapper;
	private Map<String, List<Float>> times = new HashMap<>();

	public static void main(String[] args) throws InterruptedException {
		new ManualLoadTest().run(CONCURRENT_USERS, CONCEPTS_TO_CREATE_PER_USER);
	}

	private void run(int concurrentUsers, int conceptsToClonePerUser) throws InterruptedException {
		restTemplate = new RestTemplateBuilder()
				.additionalInterceptors((ClientHttpRequestInterceptor) (request, body, execution) -> {
					request.getHeaders().setContentType(MediaType.APPLICATION_JSON);
					request.getHeaders().add("Cookie", COOKIE);
					ClientHttpResponse httpResponse = execution.execute(request, body);
					if (!(httpResponse.getRawStatusCode() + "").startsWith("2")) {
						LOGGER.info("Request failed. Request '{}'", new String(body));
						LOGGER.info("Request failed. Response {} '{}'", httpResponse.getRawStatusCode(), StreamUtils.copyToString(httpResponse.getBody(), Charset.defaultCharset()));
					}
					return httpResponse;
				})
				.rootUri(SNOWSTORM_API_URI)
				.build();

		objectMapper = Jackson2ObjectMapperBuilder
				.json()
				.defaultViewInclusion(false)
				.failOnUnknownProperties(false)
				.serializationInclusion(JsonInclude.Include.NON_NULL)
				.build();

		loadTestBranch = createBranch("MAIN", "load-test-" + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()));

		ExecutorService executorService = Executors.newCachedThreadPool();

		Set<Future> futures = new HashSet<>();
		for (int i = 0; i < concurrentUsers; i++) {
			// Start a user
			System.out.println("Create user " + (i + 1));
			futures.add(executorService.submit(new User("user-" + (i + 1), conceptsToClonePerUser)));
			// Wait before starting another
			Thread.sleep(USER_START_STAGGER * 1000);
		}
		futures.forEach(future -> {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		});

		System.out.println();
		System.out.println("Report ---");
		System.out.println(String.format("%s concurrent users (staggered by %s seconds) creating a task, searching and cloning a concept %s times",
				concurrentUsers, USER_START_STAGGER, conceptsToClonePerUser));
		for (String operation : times.keySet()) {
			List<Float> operationTimes = times.get(operation);
			if (!operationTimes.isEmpty()) {
				int sum = operationTimes.stream().mapToInt(Float::intValue).sum();
				int max = operationTimes.stream().mapToInt(Float::intValue).max().getAsInt();
				float seconds = Math.round((float) (sum * 100) / (float) operationTimes.size()) / 100f;
				System.out.println(String.format("%s average = %s seconds, max = %s (%s times)", operation, seconds, max, operationTimes.size()));
			}
		}

		executorService.shutdown();
	}

	class User implements Runnable {

		private final String username;
		private final int iterations;

		public User(String username, int iterations) {
			this.username = username;
			this.iterations = iterations;
		}

		@Override
		public void run() {
			try {
				LOGGER.info("{} starting", username);

				for (int i = 0; i < iterations; i++) {
					// Create branch
					String taskBranch = createBranch(loadTestBranch, username + "-task" + (i + 1));

					// Randomly pick a hierarchy to author in
					String hierarchy = getRandomItem(HIERARCHIES_TO_AUTHOR_IN);

					// Search for concepts
					List<ConceptResult> concepts = getConcepts(taskBranch, "<" + hierarchy);

					ConceptResult conceptMini = getRandomItem(concepts);

					// Load a random concept from search results
					Concept concept = getConcept(conceptMini.getConceptId());

					// Clone concept
					clearConceptIds(concept);
					concept.getDescriptions().stream().filter(d -> d.isActive() && d.getTypeId().equals(Concepts.FSN)).forEach(fsn -> fsn.setTerm("Cloned " + fsn.getTerm()));

					// Validate concept
					validateConcept(taskBranch, concept);

					// Save clone
					createConcept(taskBranch, concept);
				}

			} catch (Exception e) {
				LOGGER.error("User {} failed.", e);
			} finally {
				LOGGER.info("{} ended", username);
			}
		}
	}

	private void clearConceptIds(Concept concept) {
		concept.setConceptId(null);
		// Remove inactive
		concept.getDescriptions().removeAll(concept.getDescriptions().stream().filter(d -> !d.isActive()).collect(Collectors.toList()));
		concept.getDescriptions().forEach(description -> description.setDescriptionId(null));

		// Remove inactive
		concept.getRelationships().removeAll(concept.getRelationships().stream().filter(r -> !r.isActive()).collect(Collectors.toList()));
		concept.getRelationships().forEach(relationship -> relationship.setRelationshipId(null));
		// Remove not stated
		concept.getRelationships().removeAll(concept.getRelationships().stream().filter(r -> !r.getCharacteristicTypeId().equals(Concepts.STATED_RELATIONSHIP)).collect(Collectors.toList()));

		concept.getClassAxioms().forEach(axiom -> axiom.setAxiomId(null));
		concept.getGciAxioms().forEach(axiom -> axiom.setAxiomId(null));
	}

	private String createBranch(String parent, String branchName) {
		long startMilis = new Date().getTime();
		restTemplate.postForObject("/branches", json(ImmutableMap.builder()
				.put("parent", parent)
				.put("name", branchName)
				.build()), Void.class);
		String path = parent + "/" + branchName;
		LOGGER.info("Branch {} created in {} seconds", path, recordDuration("create-branch", startMilis));
		return path;
	}

	private String json(Object o) {
		try {
			return objectMapper.writeValueAsString(o);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateConcept(String taskBranch, Concept concept) throws JsonProcessingException {
		long startMilis = new Date().getTime();
		restTemplate.postForObject("/browser/" + taskBranch + "/validate/concept", conceptToString(concept), ArrayList.class);
		LOGGER.info("Validated concept {} on {} in {} seconds", concept.getFsn(), taskBranch, recordDuration("validate-concept", startMilis));
	}

	private Concept createConcept(String taskBranch, Concept concept) throws JsonProcessingException {
		long startMilis = new Date().getTime();
		Concept newConcept = restTemplate.postForObject("/browser/" + taskBranch + "/concepts", conceptToString(concept), Concept.class);
		LOGGER.info("Created concept {} on {} in {} seconds", newConcept.getConceptId(), taskBranch, recordDuration("create-concept", startMilis));
		return newConcept;
	}

	private String conceptToString(ConceptView concept) throws JsonProcessingException {
		// Manually strip the snowstorm fields so it works against SO6 too.
		return objectMapper.writeValueAsString(concept)
				.replace("\"grouped\":true,", "")
				.replace("\"grouped\":false,", "")
				.replace("classAxioms", "additionalAxioms")
				.replace(",\"acceptabilityMapFromLangRefsetMembers\":{}", "")
				.replaceAll("\"tag\":\"[a-z ]*\",", "")
				.replaceAll("\"languageCode\":\"[a-z]*\",", "")
				.replace(",\"primitive\":true,\"allOwlAxiomMembers\":[]", "")
				.replace(",\"primitive\":false,\"allOwlAxiomMembers\":[]", "")
				.replaceAll("\"termLen\":[0-9]*,", "")
				.replaceAll("\"relationshipGroup\":[0-9]*,", "")
				.replaceAll("\"caseSignificanceId\":\"[0-9]*\",", "")
				.replaceAll("\"id\":\"[0-9]*\",", "")
				.replaceAll(",\"id\":\"[0-9]*\"", "")
				.replaceAll("\"modifierId\":\"[0-9]*\",", "")
				.replaceAll("\"characteristicTypeId\":\"[0-9]*\",", "")
				.replaceAll("\"definitionStatusId\":\"[0-9]*\",", "")
				.replaceAll("\"caseSignificanceId\":\"[0-9]*\",", "")
				.replaceAll("\"typeId\":\"[0-9]*\",", "")
				.replaceAll("\"destinationId\":\"[0-9]*\",", "")
		;
	}

	private List<ConceptResult> getConcepts(String loadTestBranch, String ecl) {
		long startMilis = new Date().getTime();
		ResponseEntity<ItemsPagePojo<ConceptResult>> conceptListResponse = restTemplate.exchange("/" + loadTestBranch + "/concepts?active=true", HttpMethod.GET, null, PAGE_OF_CONCEPTS_TYPE, ImmutableMap.builder()
				.put("ecl", ecl)
				.build());
		if (!conceptListResponse.getStatusCode().is2xxSuccessful()) {
			LOGGER.error("ECL request not successful {}", conceptListResponse.getStatusCodeValue());
		}
		ItemsPagePojo<ConceptResult> page = conceptListResponse.getBody();
		List<ConceptResult> items = page.getItems();
		LOGGER.info("ECL {} fetched {} of {} concepts in {} seconds.", ecl, NumberFormat.getNumberInstance().format(items.size()), page.getTotal(), recordDuration("search-concepts", startMilis));
		return items;
	}

	private <T> T getRandomItem(List<T> items) {
		int i = ThreadLocalRandom.current().nextInt(0, items.size() - 1);
		return items.get(i);
	}

	private Concept getConcept(String conceptId) {
		long startMilis = new Date().getTime();
		String url = "/browser/" + loadTestBranch + "/concepts/" + conceptId;
		ResponseEntity<Concept> conceptResponse = restTemplate.exchange(url, HttpMethod.GET, null, Concept.class);
		if (!conceptResponse.getStatusCode().is2xxSuccessful()) {
			LOGGER.error("Concept fetch request not successful {} {}", url, conceptResponse.getStatusCodeValue());
		}
		Concept concept = conceptResponse.getBody();
		LOGGER.info("Concept {} |{}| fetched in {} seconds.", conceptId, concept.getFsn(), recordDuration("load-concept", startMilis));
		return concept;
	}

	private synchronized float recordDuration(String operation, long startMilis) {
		float seconds = Math.round((new Date().getTime() - startMilis) / 10f) / 100f;
		times.computeIfAbsent(operation, i -> new ArrayList<>()).add(seconds);
		return seconds;
	}

}

package org.snomed.snowstorm.core.data.services;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import io.kaicode.elasticvc.api.BranchCriteria;
import io.kaicode.elasticvc.api.VersionControlHelper;
import io.kaicode.elasticvc.domain.DomainEntity;
import it.unimi.dsi.fastutil.Function;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.snomed.snowstorm.core.data.domain.Concept;
import org.snomed.snowstorm.core.data.domain.Description;
import org.snomed.snowstorm.core.data.domain.Relationship;
import org.snomed.snowstorm.core.data.domain.SnomedComponent;
import org.snomed.snowstorm.rest.pojo.ConceptChangeType;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.FetchSourceFilter;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Set;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;
import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.range;
import static io.kaicode.elasticvc.api.ComponentService.LARGE_PAGE;

@Service
public class ConceptChangeReportService {

	private final VersionControlHelper versionControlHelper;
	private final ElasticsearchOperations elasticsearchOperations;

	public ConceptChangeReportService(VersionControlHelper versionControlHelper, ElasticsearchOperations elasticsearchOperations) {
		this.versionControlHelper = versionControlHelper;
		this.elasticsearchOperations = elasticsearchOperations;
	}

	public List<Long> findChangedConcepts(String branch, int changedSince, Set<ConceptChangeType> changeTypes) {
		Set<Long> conceptIds = new LongOpenHashSet();

		BranchCriteria branchCriteria = versionControlHelper.getBranchCriteria(branch);
		Query effectiveTimeGreaterThan = range().field(SnomedComponent.Fields.EFFECTIVE_TIME).gt(JsonData.of(changedSince)).build()._toQuery();

		if (changeTypes.isEmpty() || changeTypes.contains(ConceptChangeType.CONCEPT)) {
			gatherConceptIds(Concept.class, Concept.Fields.CONCEPT_ID,
					concept -> ((Concept)concept).getConceptIdAsLong(),
					effectiveTimeGreaterThan, branchCriteria, conceptIds);
		}
		if (changeTypes.isEmpty() || changeTypes.contains(ConceptChangeType.DESCRIPTION)) {
			gatherConceptIds(Description.class, Description.Fields.CONCEPT_ID,
					description -> Long.parseLong(((Description)description).getConceptId()),
					effectiveTimeGreaterThan, branchCriteria, conceptIds);
		}
		if (changeTypes.isEmpty() || changeTypes.contains(ConceptChangeType.INFERRED_RELATIONSHIP)) {
			gatherConceptIds(Relationship.class, Relationship.Fields.SOURCE_ID,
					relationship -> Long.parseLong(((Relationship)relationship).getSourceId()),
					effectiveTimeGreaterThan, branchCriteria, conceptIds);
		}

		return conceptIds.stream().sorted().toList();
	}

	private <T extends DomainEntity<?>> void gatherConceptIds(Class<T> entityClass, String conceptIdField, Function<T, Long> extractor,
			Query effectiveTimeGreaterThan, BranchCriteria branchCriteria, Set<Long> conceptIds) {
		NativeQueryBuilder builder = new NativeQueryBuilder()
				.withQuery(bool(b -> b
						.must(effectiveTimeGreaterThan)
						.must(branchCriteria.getEntityBranchCriteria(entityClass))))
				.withSourceFilter(new FetchSourceFilter(new String[]{conceptIdField}, null))
				.withPageable(LARGE_PAGE);
		try (SearchHitsIterator<T> conceptStream = elasticsearchOperations.searchForStream(builder.build(), entityClass)) {
			conceptStream.forEachRemaining(entity -> conceptIds.add(extractor.get(entity.getContent())));
		}
	}

}

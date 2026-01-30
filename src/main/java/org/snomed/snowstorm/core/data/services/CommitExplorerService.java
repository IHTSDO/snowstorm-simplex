package org.snomed.snowstorm.core.data.services;

import io.kaicode.elasticvc.domain.Branch;
import io.kaicode.elasticvc.helper.SortBuilders;
import org.snomed.snowstorm.core.data.domain.SnomedComponent;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.stream.Collectors;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;
import static io.kaicode.elasticvc.helper.QueryHelper.*;
import static java.lang.String.format;
import static org.snomed.snowstorm.core.data.domain.SnomedComponent.Fields.START;
import static org.snomed.snowstorm.core.data.domain.SnomedComponent.Fields.END;

/**
 * Local utility for exploring content within Snowstorm indices.
 */
@Service
public class CommitExplorerService {

	public static final String PATH = "path";
	private final ElasticsearchOperations operations;
	private final DomainEntityConfiguration domainEntityConfiguration;

	public CommitExplorerService(ElasticsearchOperations operations, DomainEntityConfiguration domainEntityConfiguration) {
		this.operations = operations;
		this.domainEntityConfiguration = domainEntityConfiguration;
	}

	/**
	 * List commits on a branch.
	 * @param path The branch path.
	 * @param size The number of commits to explain
	 */
	public String listCommits(String path, int size) {
		StringBuilder builder = new StringBuilder();
		SearchHits<Branch> branchVersions = operations.search(new NativeQueryBuilder()
				.withQuery(termQuery(PATH, path))
				.withSort(SortBuilders.fieldSortDesc(START))
				.withPageable(PageRequest.of(0, size))
				.build(), Branch.class);
		builder.append("Latest %s commits on %s%n".formatted(branchVersions.getSearchHits().size(), path));
		for (SearchHit<Branch> hit : branchVersions) {
			Branch branchVersion = hit.getContent();
			builder.append("%s (%s, base %s) versions replaced %s%n".formatted(branchVersion.getStartDebugFormat(),
					branchVersion.getStart().getTime(), branchVersion.getBaseTimestamp(), branchVersion.getVersionsReplacedCounts()));

			for (Class<? extends SnomedComponent<?>> componentClass : domainEntityConfiguration.getComponentTypeRepositoryMap().keySet()) {
				SearchHits<? extends SnomedComponent<?>> results = operations.search(new NativeQueryBuilder()
						.withQuery(bool(b -> b
								.must(termQuery(PATH, path))
								.must(termQuery(START, branchVersion.getHead())))
						)
						.build(), componentClass);
				long totalHits = results.getTotalHits();
				if (totalHits > 0) {
					builder.append("%s %s versions started%n".formatted(totalHits, componentClass.getSimpleName()));
				}
				results = operations.search(new NativeQueryBuilder()
						.withQuery(bool(b -> b
								.must(termQuery(PATH, path))
								.must(termQuery(END, branchVersion.getHead())))
						)
						.build(), componentClass);
				totalHits = results.getTotalHits();
				if (totalHits > 0) {
					builder.append("%s %s versions ended%n".formatted(totalHits, componentClass.getSimpleName()));
				}
			}

			final SearchHits<Branch> childBranches = operations.search(new NativeQueryBuilder()
					.withQuery(
							bool(b -> b
									.must(prefixQuery(PATH, path + "/"))
									.must(termQuery("base", branchVersion.getHead()))
									.mustNot(existsQuery("end")))
					).build(), Branch.class);
			final Set<String> childPaths = childBranches.getSearchHits().stream().map(childHit -> childHit.getContent().getPath()).collect(Collectors.toSet());
			builder.append(" > %s children with this base: %s%n".formatted(childPaths.size(), childPaths));
			builder.append("\n");
		}
		builder.append("%s total%n".formatted(branchVersions.getTotalHits()));
		return builder.toString();
	}

	/**
	 * List the last x versions of the specified SNOMED CT component across branches.
	 * Listing includes:
	 * 	- the start time of the commit (both timestamp and human readable)
	 * 	- the branch path
	 * 	- active and released states
	 * 	- component version ended state
	 * 	- the component version id and a link to the document in Elasticsearch
	 * @param id Component identifier
	 * @param componentClass Component class
	 * @param size Number of versions to return
	 */
	public <T extends SnomedComponent<?>> String listRecentVersions(String id, Class<T> componentClass, int size) {
		StringBuilder builder = new StringBuilder();
		SearchHits<T> searchHits = operations.search(new NativeQueryBuilder()
				.withQuery(termQuery(getIdField(componentClass), id))
				.withSort(SortBuilders.fieldSortDesc(START))
				.withPageable(PageRequest.of(0, size))
				.build(), componentClass);
		builder.append("Latest %s versions of %s '%s'%n".formatted(searchHits.getTotalHits(), componentClass.getSimpleName(), id));
		builder.append("\n");
		for (SearchHit<T> hit : searchHits) {
			T componentVersion = hit.getContent();
			builder.append("%s (%s) - %s - %s,%s - %s - %s%n".formatted(
					componentVersion.getStartDebugFormat(),
					componentVersion.getStart().getTime(),
					componentVersion.getPath(),
					componentVersion.isActive() ? "Active" : "Inactive",
					componentVersion.isReleased() ? "Released" : "",
					componentVersion.getEnd() == null ? "Current version on branch" : format("Ended @ %s (%s)", componentVersion.getEndDebugFormat(), componentVersion.getEnd().getTime()),
					format("ID:%s %s/%s", componentVersion.getInternalId(), getTypeMapping(componentClass), componentVersion.getInternalId())
			));
			builder.append("\n");
		}
		builder.append("%s total%n".formatted(searchHits.getTotalHits()));
		return builder.toString();
	}

	public String listComponentsOfCommit(String path, long timestamp, int size) {
		StringBuilder builder = new StringBuilder();
		SearchHits<Branch> branchVersions = operations.search(new NativeQueryBuilder()
				.withQuery(termQuery(PATH, path))
				.withQuery(termQuery(START, timestamp))
				.withPageable(PageRequest.of(0, 1))
				.build(), Branch.class);
		if (branchVersions.getTotalHits() == 0) {
			builder.append("No branch version found for path %s at %s%n".formatted(path, timestamp));
			return builder.toString();
		}

		Branch branchVersion = branchVersions.getSearchHits().get(0).getContent();
		builder.append("Component changes for %s (%s, base %s)%n"
				.formatted(path, branchVersion.getStartDebugFormat(), branchVersion.getStart().getTime()));
		builder.append("\n");

		for (Class<? extends SnomedComponent<?>> componentClass : domainEntityConfiguration.getComponentTypeRepositoryMap().keySet()) {
			final SearchHits<? extends SnomedComponent<?>> results = operations.search(new NativeQueryBuilder()
					.withQuery(bool(b -> b
							.must(termQuery(PATH, path))
							.must(bool()
									.should(termQuery(START, branchVersion.getHead()))
									.should(termQuery(END, branchVersion.getHead()))
									.build()._toQuery()))
					)
					.withSort(SortBuilders.fieldSortDesc(getIdField(componentClass)))
					.build(), componentClass);
			long totalHits = results.getTotalHits();
			if (totalHits > 0) {
				int count = 0;
				for (SearchHit<? extends SnomedComponent<?>> searchHit : results.stream().toList()) {
					SnomedComponent<?> component = searchHit.getContent();
					builder.append("- %s %s%n".formatted(component.getStart().getTime() == timestamp ? "Version Created" : "Version Ended",
							component.toString().replace("\n", "")));
					if (++count > size) {
						break;
					}
				}
				builder.append("%s of %s %s changes%n".formatted(count, totalHits, componentClass.getSimpleName()));
				builder.append("\n");
			}
		}
		return builder.toString();
	}

	private <T extends SnomedComponent<?>> String getTypeMapping(Class<T> componentClass) {
		Document annotation = componentClass.getAnnotation(Document.class);
		String indexName = annotation.indexName();
		return format("%s", indexName);
	}

	private <T extends SnomedComponent<?>> String getIdField(Class<T> componentClass) {
		try {
			return componentClass.getDeclaredConstructor().newInstance().getIdField();
		} catch (ReflectiveOperationException e) {
			throw new RuntimeServiceException(format("Not able to create instance of %s", componentClass.getSimpleName()), e);
		}
	}
}

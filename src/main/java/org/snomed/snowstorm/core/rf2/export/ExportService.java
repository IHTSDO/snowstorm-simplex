package org.snomed.snowstorm.core.rf2.export;

import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import com.google.common.collect.Sets;
import io.kaicode.elasticvc.api.BranchCriteria;
import io.kaicode.elasticvc.api.BranchService;
import io.kaicode.elasticvc.api.VersionControlHelper;
import io.kaicode.elasticvc.domain.Branch;
import org.apache.tomcat.util.http.fileupload.util.Streams;
import org.drools.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snomed.snowstorm.core.data.domain.*;
import org.snomed.snowstorm.core.data.domain.jobs.ExportConfiguration;
import org.snomed.snowstorm.core.data.domain.jobs.ExportStatus;
import org.snomed.snowstorm.core.data.repositories.ExportConfigurationRepository;
import org.snomed.snowstorm.core.data.services.*;
import org.snomed.snowstorm.core.rf2.RF2Type;
import org.snomed.snowstorm.core.util.DateUtil;
import org.snomed.snowstorm.core.util.TimerUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;
import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.range;
import static io.kaicode.elasticvc.api.ComponentService.LARGE_PAGE;
import static io.kaicode.elasticvc.helper.QueryHelper.*;
import static java.lang.String.format;

@Service
public class ExportService {

	@Autowired
	private VersionControlHelper versionControlHelper;

	@Autowired
	private ElasticsearchOperations elasticsearchOperations;

	@Autowired
	private QueryService queryService;

	@Autowired
	private ExportConfigurationRepository exportConfigurationRepository;

	@Autowired
	private BranchService branchService;

	@Autowired
	private BranchMetadataHelper branchMetadataHelper;

	@Autowired
	private ModuleDependencyService mdrService;

	@Autowired
	private CodeSystemService codeSystemService;

	@Autowired
	private ExecutorService executorService;

	private final Set<String> refsetTypesRequiredForClassification = Sets.newHashSet(Concepts.REFSET_MRCM_ATTRIBUTE_DOMAIN, Concepts.OWL_EXPRESSION_TYPE_REFERENCE_SET);

	private final Logger logger = LoggerFactory.getLogger(getClass());

	public String createJob(ExportConfiguration exportConfiguration) {
		if (exportConfiguration.getType() == RF2Type.FULL) {
			throw new IllegalArgumentException("FULL RF2 export is not implemented.");
		}
		if (exportConfiguration.getStartEffectiveTime() != null && exportConfiguration.getType() != RF2Type.SNAPSHOT) {
			throw new IllegalArgumentException("The startEffectiveTime parameter can only be used with the SNAPSHOT export type.");
		}
		branchService.findBranchOrThrow(exportConfiguration.getBranchPath());
		exportConfiguration.setId(UUID.randomUUID().toString());
		exportConfiguration.setStatus(ExportStatus.PENDING);
		if (exportConfiguration.getFilenameEffectiveDate() == null) {
			exportConfiguration.setFilenameEffectiveDate(DateUtil.DATE_STAMP_FORMAT.format(new Date()));
		}
		exportConfigurationRepository.save(exportConfiguration);
		return exportConfiguration.getId();
	}

	public ExportConfiguration getExportJobOrThrow(String exportId) {
		Optional<ExportConfiguration> config = exportConfigurationRepository.findById(exportId);
		if (config.isEmpty()) {
			throw new NotFoundException("Export job not found.");
		}
		return config.get();
	}

	public void exportRF2Archive(ExportConfiguration exportConfiguration, OutputStream outputStream) throws ExportException {
		synchronized (this) {
			if (exportConfiguration.getStartDate() != null) {
				throw new IllegalStateException("Export already started.");
			}
			exportConfiguration.setStartDate(new Date());
			exportConfiguration.setStatus(ExportStatus.RUNNING);
			exportConfigurationRepository.save(exportConfiguration);
		}

		File exportFile = exportRF2ArchiveFile(exportConfiguration.getBranchPath(), exportConfiguration.getFilenameEffectiveDate(),
				exportConfiguration.getType(), exportConfiguration.isConceptsAndRelationshipsOnly(), exportConfiguration.isUnpromotedChangesOnly(),
				exportConfiguration.getTransientEffectiveTime(), exportConfiguration.getStartEffectiveTime(), exportConfiguration.getModuleIds(),
				exportConfiguration.isLegacyZipNaming(), exportConfiguration.getRefsetIds(), exportConfiguration.getId());

		logger.info("Transmitting {} export file {}", exportConfiguration.getId(), exportFile);
		try (FileInputStream inputStream = new FileInputStream(exportFile)) {
			long fileSize = Files.size(exportFile.toPath());
			long bytesTransferred = Streams.copy(inputStream, outputStream, false);
			exportConfiguration.setStatus(ExportStatus.COMPLETED);
			exportConfigurationRepository.save(exportConfiguration);
			logger.info("Transmitted {}bytes (file size = {}bytes) for export {}", bytesTransferred, fileSize, exportConfiguration.getId());
		} catch (IOException e) {
			exportConfiguration.setStatus(ExportStatus.FAILED);
			exportConfigurationRepository.save(exportConfiguration);
			throw new ExportException("Failed to copy RF2 data into output stream.", e);
		} finally {
			if (!exportFile.delete()) {
				logger.warn("Temp export file {} could not be deleted.", exportFile);
			}
			logger.info("Deleted {} export file {}", exportConfiguration.getId(), exportFile);
		}
	}

	public File exportRF2ArchiveFile(String branchPath, String filenameEffectiveDate, RF2Type exportType, boolean forClassification) throws ExportException {
		return exportRF2ArchiveFile(branchPath, filenameEffectiveDate, exportType, forClassification, false, null, null, null, true, new HashSet<>(), null);
	}

	public void exportRF2ArchiveAsync(ExportConfiguration exportConfiguration) {
		executorService.execute(() -> {
			synchronized (this) {
				if (exportConfiguration.getStartDate() != null) {
					throw new IllegalStateException("Export already started.");
				}

				exportConfiguration.setStartDate(new Date());
				exportConfiguration.setStatus(ExportStatus.RUNNING);
				exportConfigurationRepository.save(exportConfiguration);
			}

			File file = null;
			try {
				file = exportRF2ArchiveFile(exportConfiguration.getBranchPath(), exportConfiguration.getFilenameEffectiveDate(),
						exportConfiguration.getType(), exportConfiguration.isConceptsAndRelationshipsOnly(), exportConfiguration.isUnpromotedChangesOnly(),
						exportConfiguration.getTransientEffectiveTime(), exportConfiguration.getStartEffectiveTime(), exportConfiguration.getModuleIds(),
						exportConfiguration.isLegacyZipNaming(), exportConfiguration.getRefsetIds(), exportConfiguration.getId());

				exportConfiguration.setExportFilePath(file.getAbsolutePath());
				exportConfiguration.setStatus(ExportStatus.COMPLETED);
			} catch (ExportException | SecurityException e) {
				exportConfiguration.setExportFilePath(null);
				exportConfiguration.setStatus(ExportStatus.FAILED);
			} finally {
				exportConfigurationRepository.save(exportConfiguration);
				if (file != null) {
					file.deleteOnExit();
				}
			}
		});
	}

	public void copyRF2Archive(ExportConfiguration exportConfiguration, OutputStream outputStream) {
		File archive = new File(exportConfiguration.getExportFilePath());
		if (archive.isFile()) {
			try (FileInputStream inputStream = new FileInputStream(archive)) {
				long fileSize = Files.size(archive.toPath());
				long bytesTransferred = Streams.copy(inputStream, outputStream, false);
				logger.info("Transmitted " + bytesTransferred + "bytes (file size = " + fileSize + "bytes) for export " + exportConfiguration.getId());
			} catch (IOException e) {
				throw new ExportException("Failed to copy RF2 data into output stream.", e);
			} finally {
				boolean delete = archive.delete();
				if (delete) {
					logger.error("Deleted {} export file.", exportConfiguration.getId());
					exportConfiguration.setStatus(ExportStatus.DOWNLOADED);
					exportConfigurationRepository.save(exportConfiguration);
				} else {
					logger.error("Failed to delete {} export file.", exportConfiguration.getId());
				}
			}
		} else {
			exportConfiguration.setStatus(ExportStatus.FAILED);
			exportConfigurationRepository.save(exportConfiguration);
		}
	}

	private File exportRF2ArchiveFile(String branchPath, String filenameEffectiveDate, RF2Type exportType, boolean forClassification,
			boolean unpromotedChangesOnly, String transientEffectiveTime, String startEffectiveTime, Set<String> moduleIds,
			boolean legacyZipNaming, Set<String> refsetIds, String exportId) throws ExportException {

		if (exportType == RF2Type.FULL) {
			throw new IllegalArgumentException("FULL RF2 export is not implemented.");
		}

		boolean generateMDR = exportType == RF2Type.DELTA && !StringUtils.isEmpty(transientEffectiveTime) && !unpromotedChangesOnly;

		String exportStr = exportId == null ? "" : (" - " + exportId);
		logger.info("Starting {} export of {}{}", exportType, branchPath, exportStr);
		Date startTime = new Date();

		BranchCriteria allContentBranchCriteria = versionControlHelper.getBranchCriteria(branchPath);
		BranchCriteria selectionBranchCriteria = unpromotedChangesOnly ? versionControlHelper.getChangesOnBranchCriteria(branchPath) : allContentBranchCriteria;

		String entryDirectoryPrefix = "SnomedCT_Export/RF2Release/";
		String codeSystemRF2Name = "INT";
		if (!legacyZipNaming) {
			entryDirectoryPrefix = format("SnomedCT_Export/%s/", exportType.getName());

			final CodeSystem codeSystem = codeSystemService.findClosestCodeSystemUsingAnyBranch(branchPath, false);
			if (codeSystem != null && codeSystem.getShortCode() != null) {
				codeSystemRF2Name = codeSystem.getShortCode();
			}
		}

		//Need to detect if this is an Edition or Extension package so we know what MDRS rows to export
		//Extensions only mention their own modules, despite being able to "see" those on MAIN
		Branch branch = branchService.findBranchOrThrow(branchPath, true);
		final boolean isExtension = (branch.getMetadata() != null && !StringUtils.isEmpty(branch.getMetadata().getString(BranchMetadataKeys.DEPENDENCY_PACKAGE)));
		boolean refsetOnlyExport = refsetIds != null && !refsetIds.isEmpty();

		try {
			branchService.lockBranch(branchPath, branchMetadataHelper.getBranchLockMetadata("Exporting RF2 " + exportType.getName()));
			File exportFile = File.createTempFile("export-" + new Date().getTime(), ".zip");
			try (ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(exportFile))) {
				if (!refsetOnlyExport) {
					// Write Concepts
					exportConcepts(filenameEffectiveDate, exportType, transientEffectiveTime, startEffectiveTime, moduleIds, entryDirectoryPrefix,
							zipOutputStream, selectionBranchCriteria, codeSystemRF2Name);

					if (!forClassification) {
						// Write Descriptions
						Query descriptionBranchCriteria = exportDescriptions(filenameEffectiveDate, exportType, transientEffectiveTime,
								startEffectiveTime, moduleIds, selectionBranchCriteria, entryDirectoryPrefix, zipOutputStream, codeSystemRF2Name);

						// Write Text Definitions
						exportTextDefinitions(filenameEffectiveDate, exportType, transientEffectiveTime, startEffectiveTime, moduleIds,
								descriptionBranchCriteria, entryDirectoryPrefix, zipOutputStream, codeSystemRF2Name);
					}

					// Write Stated and Inferred Relationships
					exportRelationshipsAllTypes(filenameEffectiveDate, exportType, transientEffectiveTime, startEffectiveTime, moduleIds,
							selectionBranchCriteria, entryDirectoryPrefix, zipOutputStream, codeSystemRF2Name);

					// Write Identifiers
					exportIdentifiers(filenameEffectiveDate, exportType, transientEffectiveTime, startEffectiveTime, moduleIds,
							selectionBranchCriteria, entryDirectoryPrefix, zipOutputStream, codeSystemRF2Name);
				}

				// Write Reference Sets
				exportRefsetMembers(branchPath, filenameEffectiveDate, exportType, forClassification, transientEffectiveTime, startEffectiveTime,
						moduleIds, refsetIds, allContentBranchCriteria, selectionBranchCriteria, isExtension, generateMDR, entryDirectoryPrefix,
						zipOutputStream, codeSystemRF2Name, refsetOnlyExport);
			}

			logger.info("{} export of {}{} complete in {} seconds.", exportType, branchPath, exportStr, TimerUtil.secondsSince(startTime));
			return exportFile;
		} catch (IOException e) {
			throw new ExportException("Failed to write RF2 zip file.", e);
		} finally {
			branchService.unlock(branchPath);
		}
	}

	private void exportConcepts(String filenameEffectiveDate, RF2Type exportType, String transientEffectiveTime, String startEffectiveTime,
			Set<String> moduleIds, String entryDirectoryPrefix, ZipOutputStream zipOutputStream, BranchCriteria selectionBranchCriteria,
			String codeSystemRF2Name) {

		int conceptLines = exportComponents(Concept.class, entryDirectoryPrefix,
				"Terminology/", "sct2_Concept_", filenameEffectiveDate, exportType, zipOutputStream,
				getContentQuery(exportType, moduleIds, startEffectiveTime, selectionBranchCriteria.getEntityBranchCriteria(Concept.class)).build()._toQuery(),
				transientEffectiveTime, null, codeSystemRF2Name);
		logger.info("{} concept states exported", conceptLines);
	}

	private Query exportDescriptions(String filenameEffectiveDate, RF2Type exportType, String transientEffectiveTime, String startEffectiveTime,
			Set<String> moduleIds, BranchCriteria selectionBranchCriteria, String entryDirectoryPrefix, ZipOutputStream zipOutputStream,
			String codeSystemRF2Name) {

		Query descriptionBranchCriteria = selectionBranchCriteria.getEntityBranchCriteria(Description.class);
		BoolQuery.Builder descriptionContentQuery = getContentQuery(exportType, moduleIds, startEffectiveTime, descriptionBranchCriteria);
		descriptionContentQuery.mustNot(termQuery(Description.Fields.TYPE_ID, Concepts.TEXT_DEFINITION));
		int descriptionLines = exportComponents(Description.class, entryDirectoryPrefix,
				"Terminology/", "sct2_Description_", filenameEffectiveDate, exportType, zipOutputStream,
				descriptionContentQuery.build()._toQuery(), transientEffectiveTime, null, codeSystemRF2Name);
		logger.info("{} description states exported", descriptionLines);
		return descriptionBranchCriteria;
	}

	private void exportTextDefinitions(String filenameEffectiveDate, RF2Type exportType, String transientEffectiveTime, String startEffectiveTime,
			Set<String> moduleIds, Query descriptionBranchCriteria, String entryDirectoryPrefix, ZipOutputStream zipOutputStream,
			String codeSystemRF2Name) {

		BoolQuery.Builder textDefinitionContentQuery = getContentQuery(exportType, moduleIds, startEffectiveTime, descriptionBranchCriteria);
		textDefinitionContentQuery.must(termQuery(Description.Fields.TYPE_ID, Concepts.TEXT_DEFINITION));
		int textDefinitionLines = exportComponents(Description.class, entryDirectoryPrefix,
				"Terminology/", "sct2_TextDefinition_", filenameEffectiveDate, exportType, zipOutputStream,
				textDefinitionContentQuery.build()._toQuery(), transientEffectiveTime, null, codeSystemRF2Name);
		logger.info("{} text defintion states exported", textDefinitionLines);
	}

	private void exportRelationshipsAllTypes(String filenameEffectiveDate, RF2Type exportType, String transientEffectiveTime,
			String startEffectiveTime, Set<String> moduleIds, BranchCriteria selectionBranchCriteria, String entryDirectoryPrefix,
			ZipOutputStream zipOutputStream, String codeSystemRF2Name) {

		// Write Stated Relationships
		Query relationshipBranchCritera = selectionBranchCriteria.getEntityBranchCriteria(Relationship.class);
		BoolQuery.Builder relationshipQuery = getContentQuery(exportType, moduleIds, startEffectiveTime, relationshipBranchCritera);
		relationshipQuery.must(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.STATED_RELATIONSHIP));
		int statedRelationshipLines = exportComponents(Relationship.class, entryDirectoryPrefix,
				"Terminology/", "sct2_StatedRelationship_", filenameEffectiveDate, exportType, zipOutputStream,
				relationshipQuery.build()._toQuery(), transientEffectiveTime, null, codeSystemRF2Name);
		logger.info("{} stated relationship states exported", statedRelationshipLines);

		// Write Inferred non-concrete Relationships
		relationshipQuery = getContentQuery(exportType, moduleIds, startEffectiveTime, relationshipBranchCritera);
		// Not 'stated' will include inferred and additional
		relationshipQuery.mustNot(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.STATED_RELATIONSHIP));
		relationshipQuery.must(existsQuery(Relationship.Fields.DESTINATION_ID));
		int inferredRelationshipLines = exportComponents(Relationship.class, entryDirectoryPrefix,
				"Terminology/", "sct2_Relationship_", filenameEffectiveDate, exportType, zipOutputStream,
				relationshipQuery.build()._toQuery(), transientEffectiveTime, null, codeSystemRF2Name);
		logger.info("{} inferred (non-concrete) and additional relationship states exported", inferredRelationshipLines);

		// Write Concrete Inferred Relationships
		relationshipQuery = getContentQuery(exportType, moduleIds, startEffectiveTime, relationshipBranchCritera);
		relationshipQuery.must(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.INFERRED_RELATIONSHIP));
		relationshipQuery.must(existsQuery(Relationship.Fields.VALUE));
		int inferredConcreteRelationshipLines = exportComponents(Relationship.class, entryDirectoryPrefix,
				"Terminology/", "sct2_RelationshipConcreteValues_", filenameEffectiveDate, exportType, zipOutputStream,
				relationshipQuery.build()._toQuery(), transientEffectiveTime, null, codeSystemRF2Name);
		logger.info("{} concrete inferred relationship states exported", inferredConcreteRelationshipLines);
	}

	private void exportIdentifiers(String filenameEffectiveDate, RF2Type exportType, String transientEffectiveTime, String startEffectiveTime,
			Set<String> moduleIds, BranchCriteria selectionBranchCriteria, String entryDirectoryPrefix, ZipOutputStream zipOutputStream,
			String codeSystemRF2Name) {

		BoolQuery.Builder identifierContentQuery = getContentQuery(exportType, moduleIds, startEffectiveTime,
				selectionBranchCriteria.getEntityBranchCriteria(Identifier.class));
		int identifierLines = exportComponents(Identifier.class, entryDirectoryPrefix,
				"Terminology/", "sct2_Identifier_", filenameEffectiveDate, exportType, zipOutputStream,
				identifierContentQuery.build()._toQuery(), transientEffectiveTime, null, codeSystemRF2Name);
		logger.info("{} identifier states exported", identifierLines);
	}

	private void exportRefsetMembers(String branchPath, String filenameEffectiveDate, RF2Type exportType, boolean forClassification,
			String transientEffectiveTime, String startEffectiveTime, Set<String> moduleIds, Set<String> refsetIds,
			BranchCriteria allContentBranchCriteria, BranchCriteria selectionBranchCriteria, boolean isExtension, boolean generateMDR,
			String entryDirectoryPrefix, ZipOutputStream zipOutputStream, String codeSystemRF2Name, boolean refsetOnlyExport) {

		List<ReferenceSetType> referenceSetTypes = getReferenceSetTypes(allContentBranchCriteria.getEntityBranchCriteria(ReferenceSetType.class)).stream()
				.filter(type -> !forClassification || refsetTypesRequiredForClassification.contains(type.getConceptId()))
				.collect(Collectors.toList());
		logger.info("{} Reference Set Types found for this export: {}", referenceSetTypes.size(), referenceSetTypes);

		Query memberBranchCriteria = selectionBranchCriteria.getEntityBranchCriteria(ReferenceSetMember.class);
		for (ReferenceSetType referenceSetType : referenceSetTypes) {
			List<Long> refsetsOfThisType = new ArrayList<>(queryService.findDescendantIdsAsUnion(allContentBranchCriteria, true,
					Collections.singleton(Long.parseLong(referenceSetType.getConceptId()))));
			refsetsOfThisType.add(Long.parseLong(referenceSetType.getConceptId()));
			for (Long refsetToExport : refsetsOfThisType) {
				exportRefset(branchPath, filenameEffectiveDate, exportType, refsetsOfThisType.size() > 1, transientEffectiveTime, startEffectiveTime, moduleIds, refsetIds,
						isExtension, generateMDR, entryDirectoryPrefix, zipOutputStream, codeSystemRF2Name, refsetOnlyExport, referenceSetType,
						refsetToExport, memberBranchCriteria);
			}
		}
	}

	private void exportRefset(String branchPath, String filenameEffectiveDate, RF2Type exportType, boolean manyRefsets, String transientEffectiveTime,
			String startEffectiveTime, Set<String> moduleIds, Set<String> refsetIds, boolean isExtension, boolean generateMDR,
			String entryDirectoryPrefix, ZipOutputStream zipOutputStream, String codeSystemRF2Name, boolean refsetOnlyExport,
			ReferenceSetType referenceSetType, Long refsetToExport, Query memberBranchCriteria) {

		boolean isMDRS = refsetToExport.toString().equals(Concepts.REFSET_MODULE_DEPENDENCY);
		//Export filter is pass-through when null
		ExportFilter<ReferenceSetMember> exportFilter = null;
		if (isMDRS) {
			logger.info("MDRS being exported for {} package style.", isExtension ? "extension" : "edition");
			exportFilter = rm -> mdrService.isExportable(rm, isExtension, moduleIds);
		}
		String exportDir = referenceSetType.getExportDir();
		String entryDirectory = !exportDir.startsWith("/") ? "Refset/" + exportDir + "/" : exportDir.substring(1) + "/";
		String entryFilenamePrefix = (!entryDirectory.startsWith("Terminology/") ? "der2_" : "sct2_") + referenceSetType.getFieldTypes() +
				"Refset_" + referenceSetType.getName() + (manyRefsets ? refsetToExport : "");
		if (generateMDR && isMDRS) {
			logger.info("MDR being generated rather than persisted.");
			Set<ReferenceSetMember> moduleDependencyRefsetMembers =
					mdrService.generateModuleDependencies(branchPath, transientEffectiveTime, moduleIds, exportType.equals(RF2Type.DELTA), null);

			int rowCount = exportRefsetMemberComponents(
					entryDirectoryPrefix, entryDirectory,
					entryFilenamePrefix,
					filenameEffectiveDate,
					exportType,
					zipOutputStream,
					moduleDependencyRefsetMembers,
					transientEffectiveTime,
					referenceSetType.getFieldNameList(),
					codeSystemRF2Name,
					exportFilter);
			logger.info("Exported Reference Set {} {} with {} members", refsetToExport, referenceSetType.getName(), rowCount);
		} else if (!refsetOnlyExport || refsetIds.contains(refsetToExport.toString())) {
			BoolQuery.Builder memberQueryBuilder = getContentQuery(exportType, moduleIds, startEffectiveTime, memberBranchCriteria);
			memberQueryBuilder.must(termQuery(ReferenceSetMember.Fields.REFSET_ID, refsetToExport));
			Query memberQuery = memberQueryBuilder.build()._toQuery();
			long memberCount = elasticsearchOperations.count(getNativeSearchQuery(memberQuery), ReferenceSetMember.class);
			if (memberCount > 0) {
				logger.info("Exporting Reference Set {} {} with {} members", refsetToExport, referenceSetType.getName(), memberCount);
				exportComponents(
						ReferenceSetMember.class,
						entryDirectoryPrefix, entryDirectory,
						entryFilenamePrefix,
						filenameEffectiveDate,
						exportType,
						zipOutputStream,
						memberQuery,
						transientEffectiveTime,
						referenceSetType.getFieldNameList(),
						codeSystemRF2Name);
			}
		}
	}

	public String getFilename(ExportConfiguration exportConfiguration) {
		return format("snomed-%s-%s-%s.zip",
				exportConfiguration.getBranchPath().replace("/", "_"),
				exportConfiguration.getFilenameEffectiveDate(),
				exportConfiguration.getType().getName());
	}

	private BoolQuery.Builder getContentQuery(RF2Type exportType, Set<String> moduleIds, String startEffectiveTime, Query branchCriteria) {
		BoolQuery.Builder contentQuery = bool().must(branchCriteria);
		if (exportType == RF2Type.DELTA) {
			contentQuery.mustNot(existsQuery(SnomedComponent.Fields.EFFECTIVE_TIME));
		}
		if (!CollectionUtils.isEmpty(moduleIds)) {
			contentQuery.must(termsQuery(SnomedComponent.Fields.MODULE_ID, moduleIds));
		}
		if (startEffectiveTime != null) {
			contentQuery.must(bool(b -> b
					.should(bool(bq -> bq.mustNot(existsQuery(SnomedComponent.Fields.EFFECTIVE_TIME))))
					.should(range().field(SnomedComponent.Fields.EFFECTIVE_TIME)
							.gte(JsonData.of(Integer.parseInt(startEffectiveTime))).build()._toQuery())));
		}
		return contentQuery;
	}

	private <T> int exportComponents(Class<T> componentClass, String entryDirectoryPrefix, String entryDirectory, String entryFilenamePrefix,
			String filenameEffectiveDate, RF2Type exportType, ZipOutputStream zipOutputStream, Query contentQuery, String transientEffectiveTime,
			List<String> extraFieldNames, String codeSystemRF2Name) {

		try (SearchHitsIterator<T> componentStream = elasticsearchOperations.searchForStream(getNativeSearchQuery(contentQuery), componentClass)) {
			Iterator<T> componentIterator = new Iterator<>() {
				@Override
				public boolean hasNext() {
					return componentStream.hasNext();
				}
				@Override
				public T next() {
					return componentStream.next().getContent();
				}
			};
			return doExportComponents(componentClass, entryDirectoryPrefix, entryDirectory, entryFilenamePrefix, filenameEffectiveDate, exportType,
					zipOutputStream, transientEffectiveTime, extraFieldNames, codeSystemRF2Name, null, componentIterator);
		}
	}

	private int exportRefsetMemberComponents(String entryDirectoryPrefix, String entryDirectory, String entryFilenamePrefix,
			String filenameEffectiveDate, RF2Type exportType, ZipOutputStream zipOutputStream, Set<ReferenceSetMember> components, String transientEffectiveTime,
			List<String> extraFieldNames, String codeSystemRF2Name, ExportFilter<ReferenceSetMember> exportFilter) {

		return doExportComponents(ReferenceSetMember.class, entryDirectoryPrefix, entryDirectory, entryFilenamePrefix, filenameEffectiveDate, exportType,
				zipOutputStream, transientEffectiveTime, extraFieldNames, codeSystemRF2Name, exportFilter, components.iterator());
	}

	private <T> int doExportComponents(Class<T> componentClass, String entryDirectoryPrefix, String entryDirectory, String entryFilenamePrefix,
			String filenameEffectiveDate, RF2Type exportType, ZipOutputStream zipOutputStream, String transientEffectiveTime,
			List<String> extraFieldNames, String codeSystemRF2Name, ExportFilter<T> exportFilter, Iterator<T> componentIterator) {

		String componentFilePath = entryDirectoryPrefix + entryDirectory + entryFilenamePrefix + format("%s_%s_%s.txt", exportType.getName(), codeSystemRF2Name, filenameEffectiveDate);
		logger.info("Exporting file {}", componentFilePath);
		logger.info("Export filter is {}", exportFilter == null ? "null" : "present");
		try {
			// Open zip entry
			zipOutputStream.putNextEntry(new ZipEntry(componentFilePath));

			// Stream components into zip
			try (ExportWriter<T> writer = getExportWriter(componentClass, zipOutputStream, extraFieldNames, entryFilenamePrefix.contains("Concrete"));
			) {
				writer.setTransientEffectiveTime(transientEffectiveTime);
				writer.writeHeader();
				componentIterator.forEachRemaining(component -> doFilteredWrite(exportFilter, writer, component));
				return writer.getContentLinesWritten();
			} finally {
				// Close zip entry
				zipOutputStream.closeEntry();
			}
		} catch (IOException e) {
			throw new ExportException("Failed to write export zip entry '" + componentFilePath + "'", e);
		}
	}

	private <T> void doFilteredWrite(ExportFilter<T> exportFilter, ExportWriter<T> writer, T item) {
		if (exportFilter == null || exportFilter.isValid(item)) {
			writer.write(item);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> ExportWriter<T> getExportWriter(Class<T> componentClass, OutputStream outputStream, List<String> extraFieldNames, boolean concrete) {
		if (componentClass.equals(Concept.class)) {
			return (ExportWriter<T>) new ConceptExportWriter(getBufferedWriter(outputStream));
		}
		if (componentClass.equals(Description.class)) {
			return (ExportWriter<T>) new DescriptionExportWriter(getBufferedWriter(outputStream));
		}
		if (componentClass.equals(Relationship.class)) {
			return (ExportWriter<T>) (concrete ? new ConcreteRelationshipExportWriter(getBufferedWriter(outputStream)) : new RelationshipExportWriter(getBufferedWriter(outputStream)));
		}
		if (componentClass.equals(ReferenceSetMember.class)) {
			return (ExportWriter<T>) new ReferenceSetMemberExportWriter(getBufferedWriter(outputStream), extraFieldNames);
		}
		if (componentClass.equals(Identifier.class)) {
			return (ExportWriter<T>) new IdentifierExportWriter(getBufferedWriter(outputStream));
		}
		throw new UnsupportedOperationException("Not able to export component of type " + componentClass.getCanonicalName());
	}

	private List<ReferenceSetType> getReferenceSetTypes(Query branchCriteria) {
		BoolQuery.Builder contentQuery = getContentQuery(RF2Type.SNAPSHOT, null, null, branchCriteria);
		return elasticsearchOperations.search(new NativeQueryBuilder()
				.withQuery(contentQuery.build()._toQuery())
				.withSort(SortOptions.of(s -> s.field(f -> f.field(ReferenceSetType.Fields.NAME))))
				.withPageable(LARGE_PAGE)
				.build(), ReferenceSetType.class)
				.stream().map(SearchHit::getContent).collect(Collectors.toList());
	}

	private NativeQuery getNativeSearchQuery(Query contentQuery) {
		return new NativeQueryBuilder()
				.withQuery(contentQuery)
				.withPageable(LARGE_PAGE)
				.build();
	}

	private BufferedWriter getBufferedWriter(OutputStream outputStream) {
		return new BufferedWriter(new OutputStreamWriter(outputStream));
	}

}

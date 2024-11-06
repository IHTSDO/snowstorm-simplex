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
import org.jetbrains.annotations.NotNull;
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
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.bool;
import static co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders.range;
import static io.kaicode.elasticvc.api.ComponentService.LARGE_PAGE;
import static io.kaicode.elasticvc.helper.QueryHelper.*;
import static java.lang.String.format;

@Service
public class ExportService {

	public static final String TERMINOLOGY_DIR = "Terminology/";

	private final VersionControlHelper versionControlHelper;
	private final ElasticsearchOperations elasticsearchOperations;
	private final QueryService queryService;
	private final ExportConfigurationRepository exportConfigurationRepository;
	private final BranchService branchService;
	private final BranchMetadataHelper branchMetadataHelper;
	private final ModuleDependencyService mdrService;
	private final CodeSystemService codeSystemService;
	private final ExecutorService executorService;

	public ExportService(VersionControlHelper versionControlHelper, ElasticsearchOperations elasticsearchOperations, QueryService queryService,
			ExportConfigurationRepository exportConfigurationRepository, BranchService branchService, BranchMetadataHelper branchMetadataHelper,
			ModuleDependencyService mdrService, CodeSystemService codeSystemService, ExecutorService executorService) {

		this.versionControlHelper = versionControlHelper;
		this.elasticsearchOperations = elasticsearchOperations;
		this.queryService = queryService;
		this.exportConfigurationRepository = exportConfigurationRepository;
		this.branchService = branchService;
		this.branchMetadataHelper = branchMetadataHelper;
		this.mdrService = mdrService;
		this.codeSystemService = codeSystemService;
		this.executorService = executorService;
	}

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

		File exportFile = exportRF2ArchiveFile(exportConfiguration);

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
			try {
				Files.delete(exportFile.toPath());
			} catch (IOException e) {
				logger.warn("Temp export file {} could not be deleted.", exportFile, e);
			}
			logger.info("Deleted {} export file {}", exportConfiguration.getId(), exportFile);
		}
	}

	public File exportRF2ClassificationDelta(String branchPath, String filenameEffectiveDate) throws ExportException {
		ExportConfiguration exportConfiguration = new ExportConfiguration(branchPath, RF2Type.DELTA);
		exportConfiguration.setFilenameEffectiveDate(filenameEffectiveDate);
		exportConfiguration.setConceptsAndRelationshipsOnly(true);
		exportConfiguration.setLegacyZipNaming(true);
		return exportRF2ArchiveFile(exportConfiguration);
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
				file = exportRF2ArchiveFile(exportConfiguration);

				exportRF2ArchiveFile(exportConfiguration);

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
				logger.info("Transmitted {}bytes (file size = {}bytes) for export {}", bytesTransferred, fileSize, exportConfiguration.getId());
			} catch (IOException e) {
				throw new ExportException("Failed to copy RF2 data into output stream.", e);
			} finally {
				try {
					Files.delete(archive.toPath());
				} catch (IOException e) {
					logger.warn("Temp export file {} could not be deleted.", archive, e);
				}
				exportConfiguration.setStatus(ExportStatus.DOWNLOADED);
				exportConfigurationRepository.save(exportConfiguration);
			}
		} else {
			exportConfiguration.setStatus(ExportStatus.FAILED);
			exportConfigurationRepository.save(exportConfiguration);
		}
	}

	private File exportRF2ArchiveFile(ExportConfiguration exportConfiguration) {

		RF2Type exportType = exportConfiguration.getType();
		if (exportType == RF2Type.FULL) {
			throw new IllegalArgumentException("FULL RF2 export is not implemented.");
		}

		String exportId = exportConfiguration.getId();
		String exportStr = exportId == null ? "" : (" - " + exportId);
		String branchPath = exportConfiguration.getBranchPath();
		logger.info("Starting {} export of {}{}", exportType, branchPath, exportStr);
		Date startTime = new Date();

		BranchCriteria allContentBranchCriteria = versionControlHelper.getBranchCriteria(branchPath);
		BranchCriteria selectionBranchCriteria = exportConfiguration.isUnpromotedChangesOnly() ?
				versionControlHelper.getChangesOnBranchCriteria(branchPath) : allContentBranchCriteria;

		String entryDirectoryPrefix = "SnomedCT_Export/RF2Release/";
		String codeSystemRF2Name = "INT";
		if (!exportConfiguration.isLegacyZipNaming()) {
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
		Set<String> refsetIds = exportConfiguration.getRefsetIds();
		if (refsetIds == null) {
			refsetIds = new HashSet<>();
			exportConfiguration.setRefsetIds(refsetIds);
		}

		try {
			branchService.lockBranch(branchPath, branchMetadataHelper.getBranchLockMetadata("Exporting RF2 " + exportType.getName()));
			File exportFile = File.createTempFile("export-" + new Date().getTime(), ".zip");
			try (ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(exportFile))) {
				if (refsetIds.isEmpty()) {
					// Write Concepts
					exportConcepts(exportConfiguration, entryDirectoryPrefix, zipOutputStream, selectionBranchCriteria, codeSystemRF2Name);

					if (!exportConfiguration.isForClassification()) {
						// Write Descriptions
						Query descriptionBranchCriteria = exportDescriptions(exportConfiguration, selectionBranchCriteria, entryDirectoryPrefix,
								zipOutputStream, codeSystemRF2Name);

						// Write Text Definitions
						exportTextDefinitions(exportConfiguration, descriptionBranchCriteria, entryDirectoryPrefix,
								zipOutputStream, codeSystemRF2Name);
					}

					// Write Stated and Inferred Relationships
					exportRelationshipsAllTypes(exportConfiguration, selectionBranchCriteria, entryDirectoryPrefix,
							zipOutputStream, codeSystemRF2Name);

					// Write Identifiers
					exportIdentifiers(exportConfiguration, selectionBranchCriteria, entryDirectoryPrefix,
							zipOutputStream, codeSystemRF2Name);
				}

				// Write Reference Sets
				FilepathDetails filepathDetails = new FilepathDetails(exportConfiguration.getFilenameEffectiveDate(), entryDirectoryPrefix, codeSystemRF2Name);
				exportRefsetMembers(exportConfiguration, filepathDetails, allContentBranchCriteria, selectionBranchCriteria, isExtension, zipOutputStream);
			}

			if (logger.isInfoEnabled()) {
				logger.info("{} export of {}{} complete in {} seconds.", exportType, branchPath, exportStr, TimerUtil.secondsSince(startTime));
			}
			return exportFile;
		} catch (IOException e) {
			throw new ExportException("Failed to write RF2 zip file.", e);
		} finally {
			branchService.unlock(branchPath);
		}
	}

	private record FilepathDetails(String filenameEffectiveDate, String entryDirectoryPrefix, String codeSystemRF2Name) {
	}

	private void exportConcepts(ExportConfiguration exportConfiguration, String entryDirectoryPrefix,
			ZipOutputStream zipOutputStream, BranchCriteria selectionBranchCriteria, String codeSystemRF2Name) {

		String componentFilePath = getComponentFilePath(exportConfiguration, entryDirectoryPrefix, TERMINOLOGY_DIR,
				"sct2_Concept_", codeSystemRF2Name);

		int conceptLines = exportComponents(exportConfiguration, Concept.class,
				zipOutputStream,
				getContentQuery(exportConfiguration, selectionBranchCriteria.getEntityBranchCriteria(Concept.class)).build()._toQuery(),
				null, componentFilePath);
		logger.info("{} concept states exported", conceptLines);
	}

	private Query exportDescriptions(ExportConfiguration exportConfiguration, BranchCriteria selectionBranchCriteria, String entryDirectoryPrefix,
			ZipOutputStream zipOutputStream, String codeSystemRF2Name) {

		Query descriptionBranchCriteria = selectionBranchCriteria.getEntityBranchCriteria(Description.class);
		BoolQuery.Builder descriptionContentQuery = getContentQuery(exportConfiguration, descriptionBranchCriteria);
		descriptionContentQuery.mustNot(termQuery(Description.Fields.TYPE_ID, Concepts.TEXT_DEFINITION));
		String componentFilePath = getComponentFilePath(exportConfiguration, entryDirectoryPrefix, TERMINOLOGY_DIR,
				"sct2_Description_", codeSystemRF2Name);
		int descriptionLines = exportComponents(exportConfiguration, Description.class, zipOutputStream,
				descriptionContentQuery.build()._toQuery(), null, componentFilePath);
		logger.info("{} description states exported", descriptionLines);
		return descriptionBranchCriteria;
	}

	private void exportTextDefinitions(ExportConfiguration exportConfiguration,	Query descriptionBranchCriteria, String entryDirectoryPrefix,
			ZipOutputStream zipOutputStream, String codeSystemRF2Name) {

		BoolQuery.Builder textDefinitionContentQuery = getContentQuery(exportConfiguration, descriptionBranchCriteria);
		textDefinitionContentQuery.must(termQuery(Description.Fields.TYPE_ID, Concepts.TEXT_DEFINITION));
		String componentFilePath = getComponentFilePath(exportConfiguration, entryDirectoryPrefix, TERMINOLOGY_DIR,
				"sct2_TextDefinition_", codeSystemRF2Name);
		int textDefinitionLines = exportComponents(exportConfiguration, Description.class, zipOutputStream,
				textDefinitionContentQuery.build()._toQuery(), null, componentFilePath);
		logger.info("{} text definition states exported", textDefinitionLines);
	}

	private void exportRelationshipsAllTypes(ExportConfiguration exportConfiguration, BranchCriteria selectionBranchCriteria, String entryDirectoryPrefix,
			ZipOutputStream zipOutputStream, String codeSystemRF2Name) {

		// Write Stated Relationships
		Query relationshipBranchCritera = selectionBranchCriteria.getEntityBranchCriteria(Relationship.class);
		BoolQuery.Builder relationshipQuery = getContentQuery(exportConfiguration, relationshipBranchCritera);
		relationshipQuery.must(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.STATED_RELATIONSHIP));
		String componentFilePathStatedRelationships = getComponentFilePath(exportConfiguration, entryDirectoryPrefix, TERMINOLOGY_DIR,
				"sct2_StatedRelationship_", codeSystemRF2Name);
		int statedRelationshipLines = exportComponents(exportConfiguration, Relationship.class, zipOutputStream,
				relationshipQuery.build()._toQuery(), null, componentFilePathStatedRelationships);
		logger.info("{} stated relationship states exported", statedRelationshipLines);

		// Write Inferred non-concrete Relationships
		relationshipQuery = getContentQuery(exportConfiguration, relationshipBranchCritera);
		// Not 'stated' will include inferred and additional
		relationshipQuery.mustNot(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.STATED_RELATIONSHIP));
		relationshipQuery.must(existsQuery(Relationship.Fields.DESTINATION_ID));
		String componentFilePathRelationships = getComponentFilePath(exportConfiguration, entryDirectoryPrefix, TERMINOLOGY_DIR,
				"sct2_Relationship_", codeSystemRF2Name);
		int inferredRelationshipLines = exportComponents(exportConfiguration, Relationship.class, zipOutputStream,
				relationshipQuery.build()._toQuery(), null, componentFilePathRelationships);
		logger.info("{} inferred (non-concrete) and additional relationship states exported", inferredRelationshipLines);

		// Write Concrete Inferred Relationships
		relationshipQuery = getContentQuery(exportConfiguration, relationshipBranchCritera);
		relationshipQuery.must(termQuery(Relationship.Fields.CHARACTERISTIC_TYPE_ID, Concepts.INFERRED_RELATIONSHIP));
		relationshipQuery.must(existsQuery(Relationship.Fields.VALUE));
		String componentFilePathConcreteRelationships = getComponentFilePath(exportConfiguration, entryDirectoryPrefix, TERMINOLOGY_DIR,
				"sct2_RelationshipConcreteValues_", codeSystemRF2Name);
		int inferredConcreteRelationshipLines = exportComponents(exportConfiguration, Relationship.class, zipOutputStream,
				relationshipQuery.build()._toQuery(), null, componentFilePathConcreteRelationships);
		logger.info("{} concrete inferred relationship states exported", inferredConcreteRelationshipLines);
	}

	private void exportIdentifiers(ExportConfiguration exportConfiguration, BranchCriteria selectionBranchCriteria, String entryDirectoryPrefix,
			ZipOutputStream zipOutputStream, String codeSystemRF2Name) {

		BoolQuery.Builder identifierContentQuery = getContentQuery(exportConfiguration, selectionBranchCriteria.getEntityBranchCriteria(Identifier.class));
		String componentFilePath = getComponentFilePath(exportConfiguration, entryDirectoryPrefix, TERMINOLOGY_DIR,
				"sct2_Identifier_", codeSystemRF2Name);
		int identifierLines = exportComponents(exportConfiguration, Identifier.class, zipOutputStream,
				identifierContentQuery.build()._toQuery(), null, componentFilePath);
		logger.info("{} identifier states exported", identifierLines);
	}

	private void exportRefsetMembers(ExportConfiguration exportConfiguration, FilepathDetails filepathDetails,
			BranchCriteria allContentBranchCriteria, BranchCriteria selectionBranchCriteria, boolean isExtension, ZipOutputStream zipOutputStream) {

		List<ReferenceSetType> referenceSetTypes = getReferenceSetTypesForExport(exportConfiguration, allContentBranchCriteria);
		logger.info("{} Reference Set Types found for this export: {}", referenceSetTypes.size(), referenceSetTypes);

		for (ReferenceSetType referenceSetType : referenceSetTypes) {
			exportRefsetMembersOfType(exportConfiguration, filepathDetails, allContentBranchCriteria, selectionBranchCriteria,
					isExtension, zipOutputStream, referenceSetType);
		}
	}

	private void exportRefsetMembersOfType(ExportConfiguration exportConfiguration, FilepathDetails filepathDetails,
			BranchCriteria allContentBranchCriteria, BranchCriteria selectionBranchCriteria,
			boolean isExtension, ZipOutputStream zipOutputStream, ReferenceSetType referenceSetType) {

		Query memberBranchCriteria = selectionBranchCriteria.getEntityBranchCriteria(ReferenceSetMember.class);
		boolean generateMDR = isGenerateMDR(exportConfiguration);
		List<Long> refsetsOfThisType = new ArrayList<>(queryService.findDescendantIdsAsUnion(allContentBranchCriteria, true,
				Collections.singleton(Long.parseLong(referenceSetType.getConceptId()))));
		refsetsOfThisType.add(Long.parseLong(referenceSetType.getConceptId()));
		for (Long refsetToExport : refsetsOfThisType) {
			String refsetFilePath = getRefsetFilePath(exportConfiguration, filepathDetails, refsetsOfThisType.size() > 1,
					referenceSetType, refsetToExport);

			boolean isMDRS = refsetToExport.toString().equals(Concepts.REFSET_MODULE_DEPENDENCY);
			//Export filter is pass-through when null
			ExportFilter<ReferenceSetMember> exportFilter = null;
			Set<String> moduleIds = exportConfiguration.getModuleIds();
			if (isMDRS) {
				logger.info("MDRS being exported for {} package style.", isExtension ? "extension" : "edition");
				exportFilter = rm -> mdrService.isExportable(rm, isExtension, moduleIds);
			}
			Set<String> refsetIds = exportConfiguration.getRefsetIds();
			if (generateMDR && isMDRS) {
				generateAndExportModuleDependencyRefset(exportConfiguration, selectionBranchCriteria, zipOutputStream, referenceSetType,
						exportFilter, refsetFilePath);
			} else if (refsetIds.isEmpty() || refsetIds.contains(refsetToExport.toString())) {
				Query memberQuery = getMemberQuery(exportConfiguration, refsetToExport, memberBranchCriteria);
				exportRefset(exportConfiguration, zipOutputStream, referenceSetType, refsetToExport, memberQuery, refsetFilePath);
			}
		}
	}

	private static boolean isGenerateMDR(ExportConfiguration exportConfiguration) {
		return exportConfiguration.getType() == RF2Type.DELTA && !StringUtils.isEmpty(exportConfiguration.getTransientEffectiveTime())
				&& !exportConfiguration.isUnpromotedChangesOnly();
	}

	private Query getMemberQuery(ExportConfiguration exportConfiguration, Long refsetToExport, Query memberBranchCriteria) {
		BoolQuery.Builder memberQueryBuilder = getContentQuery(exportConfiguration, memberBranchCriteria);
		memberQueryBuilder.must(termQuery(ReferenceSetMember.Fields.REFSET_ID, refsetToExport));
		return memberQueryBuilder.build()._toQuery();
	}

	private void generateAndExportModuleDependencyRefset(ExportConfiguration exportConfiguration, BranchCriteria selectionBranchCriteria,
			ZipOutputStream zipOutputStream, ReferenceSetType referenceSetType, ExportFilter<ReferenceSetMember> exportFilter, String refsetFilePath) {

		logger.info("MDR being generated rather than persisted.");
		String branchPath = selectionBranchCriteria.getBranchPath();
		String transientEffectiveTime = exportConfiguration.getTransientEffectiveTime();
		Set<String> moduleIds = exportConfiguration.getModuleIds();
		Set<ReferenceSetMember> moduleDependencyRefsetMembers =
				mdrService.generateModuleDependencies(branchPath, transientEffectiveTime, moduleIds, true, null);
		int rowCount = exportRefsetMemberComponents(zipOutputStream, moduleDependencyRefsetMembers, transientEffectiveTime,
				referenceSetType.getFieldNameList(), exportFilter, refsetFilePath);
		logger.info("Exported Reference Set {} {} with {} members", Concepts.REFSET_MODULE_DEPENDENCY, referenceSetType.getName(), rowCount);
	}

	private @NotNull List<ReferenceSetType> getReferenceSetTypesForExport(ExportConfiguration exportConfiguration, BranchCriteria allContentBranchCriteria) {
		List<ReferenceSetType> allReferenceSetTypes = getReferenceSetTypes(allContentBranchCriteria.getEntityBranchCriteria(ReferenceSetType.class));
		if (allReferenceSetTypes.isEmpty()) {
			// Probably using a codesystem hanging of the special empty 2000 branch.
			// Take types from MAIN
			BranchCriteria mainBranchCriteria = versionControlHelper.getBranchCriteria("MAIN");
			allReferenceSetTypes = getReferenceSetTypes(mainBranchCriteria.getEntityBranchCriteria(ReferenceSetType.class));
		}
		return allReferenceSetTypes.stream()
				.filter(type -> !exportConfiguration.isForClassification() || refsetTypesRequiredForClassification.contains(type.getConceptId()))
				.toList();
	}

	private void exportRefset(ExportConfiguration exportConfiguration, ZipOutputStream zipOutputStream, ReferenceSetType referenceSetType,
			Long refsetToExport, Query memberQuery, String componentFilePath) {

		long memberCount = elasticsearchOperations.count(getNativeSearchQuery(memberQuery), ReferenceSetMember.class);
		if (memberCount > 0) {
			logger.info("Exporting Reference Set {} {} with {} members", refsetToExport, referenceSetType.getName(), memberCount);
			exportComponents(exportConfiguration, ReferenceSetMember.class, zipOutputStream, memberQuery, referenceSetType.getFieldNameList(),
					componentFilePath);
		}
	}

	private static String getRefsetFilePath(ExportConfiguration exportConfiguration, FilepathDetails filepathDetails, boolean manyRefsets,
			ReferenceSetType referenceSetType, Long refsetToExport) {

		String exportDir = referenceSetType.getExportDir();
		String entryDirectory = !exportDir.startsWith("/") ? "Refset/" + exportDir + "/" : exportDir.substring(1) + "/";
		String entryFilenamePrefix = (!entryDirectory.startsWith(TERMINOLOGY_DIR) ? "der2_" : "sct2_") + referenceSetType.getFieldTypes() +
				"Refset_" + referenceSetType.getName() + (manyRefsets ? refsetToExport : "");
		return getComponentFilePath(exportConfiguration, filepathDetails.entryDirectoryPrefix, entryDirectory, entryFilenamePrefix,
				filepathDetails.codeSystemRF2Name);
	}

	public String getFilename(ExportConfiguration exportConfiguration) {
		return format("snomed-%s-%s-%s.zip",
				exportConfiguration.getBranchPath().replace("/", "_"),
				exportConfiguration.getFilenameEffectiveDate(),
				exportConfiguration.getType().getName());
	}

	private BoolQuery.Builder getContentQuery(ExportConfiguration exportConfiguration, Query branchCriteria) {
		return getContentQuery(exportConfiguration.getType(), exportConfiguration.getModuleIds(),
				exportConfiguration.getStartEffectiveTime(), branchCriteria);
	}

	private BoolQuery.Builder getContentQuery(RF2Type type, Set<String> moduleIds, String startEffectiveTime, Query branchCriteria) {
		BoolQuery.Builder contentQuery = bool().must(branchCriteria);
		if (type == RF2Type.DELTA) {
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

	private <T> int exportComponents(ExportConfiguration exportConfiguration, Class<T> componentClass, ZipOutputStream zipOutputStream,
			Query contentQuery, List<String> extraFieldNames, String componentFilePath) {

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
			String transientEffectiveTime = exportConfiguration.getTransientEffectiveTime();
			return doExportComponents(componentClass, zipOutputStream, transientEffectiveTime, extraFieldNames, null, componentIterator, componentFilePath);
		}
	}

	private static @NotNull String getComponentFilePath(ExportConfiguration exportConfiguration, String entryDirectoryPrefix, String entryDirectory, String entryFilenamePrefix, String codeSystemRF2Name) {
		return entryDirectoryPrefix + entryDirectory + entryFilenamePrefix + format("%s_%s_%s.txt", exportConfiguration.getType().getName(),
				codeSystemRF2Name, exportConfiguration.getFilenameEffectiveDate());
	}

	private int exportRefsetMemberComponents(ZipOutputStream zipOutputStream, Set<ReferenceSetMember> components, String transientEffectiveTime,
			List<String> extraFieldNames, ExportFilter<ReferenceSetMember> exportFilter, String componentFilePath) {

		return doExportComponents(ReferenceSetMember.class,
				zipOutputStream, transientEffectiveTime, extraFieldNames, exportFilter, components.iterator(), componentFilePath);
	}

	private <T> int doExportComponents(Class<T> componentClass, ZipOutputStream zipOutputStream, String transientEffectiveTime,
			List<String> extraFieldNames, ExportFilter<T> exportFilter, Iterator<T> componentIterator, String componentFilePath) {

		logger.info("Exporting file {}", componentFilePath);
		logger.info("Export filter is {}", exportFilter == null ? "null" : "present");
		try {
			// Open zip entry
			zipOutputStream.putNextEntry(new ZipEntry(componentFilePath));

			// Stream components into zip
			boolean concreteRelationships = componentClass.equals(Relationship.class) && componentFilePath.contains("sct2_RelationshipConcrete");
			try (ExportWriter<T> writer = getExportWriter(componentClass, zipOutputStream, extraFieldNames, concreteRelationships);
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
				.stream().map(SearchHit::getContent).toList();
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

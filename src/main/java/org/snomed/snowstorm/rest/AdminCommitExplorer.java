package org.snomed.snowstorm.rest;

import io.kaicode.rest.util.branchpathrewrite.BranchPathUriUtil;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.snomed.snowstorm.core.data.domain.Concept;
import org.snomed.snowstorm.core.data.services.CommitExplorerService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@Tag(name = "Admin - Commit Explorer", description = "-")
@RequestMapping(value = "/admin/versions", produces = "text/plain")
public class AdminCommitExplorer {

	private final CommitExplorerService commitExplorerService;

	public AdminCommitExplorer(CommitExplorerService commitExplorerService) {
		this.commitExplorerService = commitExplorerService;
	}

	@Operation(summary = "List commits on a branch.")
	@GetMapping(value = "/branch/{branch}")
	@PreAuthorize("hasPermission('ADMIN', 'global')")
	public String listCommits(@PathVariable String branch, @RequestParam(defaultValue = "10") int size) {
		branch = BranchPathUriUtil.decodePath(branch);
		return commitExplorerService.listCommits(branch, size);
	}

	@Operation(summary = "List the component changes of a commit.")
	@GetMapping(value = "/branch/{branch}/components/{timestamp}")
	@PreAuthorize("hasPermission('ADMIN', 'global')")
	public String listCommitComponents(@PathVariable String branch, @PathVariable long timestamp, @RequestParam(defaultValue = "100") int size) {
		branch = BranchPathUriUtil.decodePath(branch);
		return commitExplorerService.listComponentsOfCommit(branch, timestamp, size);
	}

	@Operation(summary = "List the versions of a concept, across all branches.")
	@GetMapping(value = "/concept/{id}")
	@PreAuthorize("hasPermission('ADMIN', 'global')")
	public String listVersionsConcept(@PathVariable String id, @RequestParam(defaultValue = "20") int size) {
		return commitExplorerService.listRecentVersions(id, Concept.class, size);
	}

}

package org.snomed.snowstorm.core.data.services.pojo;

import java.util.Date;

public class AuthoringStatsSummary {
	private long newConceptsCount;
	private long inactivatedConceptsCount;
	private long reactivatedConceptsCount;
	private long changedFsnCount;
	private long inactivatedSynonymsCount;
	private long newSynonymsForExistingConceptsCount;
	private long reactivatedSynonymsCount;
	private long newRefsets;
	private long refsetWithChangedMembers;
	private final Date executionTime;
	private String title;

	public AuthoringStatsSummary(Date time) {
		this.executionTime = time;
	}

	public void setNewConceptsCount(long newConceptsCount) {
		this.newConceptsCount = newConceptsCount;
	}

	public long getNewConceptsCount() {
		return newConceptsCount;
	}

	public void setInactivatedConceptsCount(long inactivatedConceptsCount) {
		this.inactivatedConceptsCount = inactivatedConceptsCount;
	}

	public long getInactivatedConceptsCount() {
		return inactivatedConceptsCount;
	}

	public void setReactivatedConceptsCount(long reactivatedConceptsCount) {
		this.reactivatedConceptsCount = reactivatedConceptsCount;
	}

	public long getReactivatedConceptsCount() {
		return reactivatedConceptsCount;
	}

	public void setChangedFsnCount(long changedFsnCount) {
		this.changedFsnCount = changedFsnCount;
	}

	public long getChangedFsnCount() {
		return changedFsnCount;
	}

	public void setInactivatedSynonymsCount(long inactivatedSynonymsCount) {
		this.inactivatedSynonymsCount = inactivatedSynonymsCount;
	}

	public long getInactivatedSynonymsCount() {
		return inactivatedSynonymsCount;
	}

	public void setNewSynonymsForExistingConceptsCount(long newSynonymsForExistingConceptsCount) {
		this.newSynonymsForExistingConceptsCount = newSynonymsForExistingConceptsCount;
	}

	public long getNewSynonymsForExistingConceptsCount() {
		return newSynonymsForExistingConceptsCount;
	}

	public void setReactivatedSynonymsCount(long reactivatedSynonymsCount) {
		this.reactivatedSynonymsCount = reactivatedSynonymsCount;
	}

	public long getReactivatedSynonymsCount() {
		return reactivatedSynonymsCount;
	}

	public long getNewRefsets() {
		return newRefsets;
	}

	public void setNewRefsets(long newRefsets) {
		this.newRefsets = newRefsets;
	}

	public long getRefsetWithChangedMembers() {
		return refsetWithChangedMembers;
	}

	public void setRefsetWithChangedMembers(long refsetWithChangedMembers) {
		this.refsetWithChangedMembers = refsetWithChangedMembers;
	}

	public Date getExecutionTime() {
		return this.executionTime;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

}

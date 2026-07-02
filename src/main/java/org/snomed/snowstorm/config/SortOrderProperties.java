package org.snomed.snowstorm.config;

import java.util.HashMap;
import java.util.Map;

public class SortOrderProperties {

	private final Map<String, String> attribute = new HashMap<>();
	private Map<String, Map<Long, Short>> domainAttributeOrderMap;

	public Map<String, String> getAttribute() {
		return attribute;
	}

	public Map<String, Map<Long, Short>> getDomainAttributeOrderMap() {
		if (domainAttributeOrderMap == null) {
			synchronized (this) {
				domainAttributeOrderMap = new HashMap<>();
				try {
					for (Map.Entry<String, String> entry : attribute.entrySet()) {
						String[] parts = entry.getKey().split("\\.");
						String semanticTag = parts[0];
						Long attributeId = Long.parseLong(parts[1]);
						short order = Short.parseShort(entry.getValue());
						domainAttributeOrderMap.computeIfAbsent(semanticTag, id -> new HashMap<>())
								.put(attributeId, order);
					}
				} catch (NullPointerException | NumberFormatException e) {
					throw new IllegalArgumentException("Failed to process attribute sort order configuration. Please check format.", e);
				}
			}
		}
		return domainAttributeOrderMap;
	}
}

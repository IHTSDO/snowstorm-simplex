package org.snomed.snowstorm.core.data.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.base.Strings;
import org.snomed.snowstorm.rest.View;
import org.springframework.data.annotation.Transient;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.util.Objects;

import static org.snomed.snowstorm.core.data.domain.Concepts.*;

@Document(indexName = "#{@indexNameProvider.indexName('relationship')}", createIndex = false)
public class Relationship extends SnomedComponent<Relationship> {

	public enum CharacteristicType {

		inferred(Concepts.INFERRED_RELATIONSHIP),
		stated(Concepts.STATED_RELATIONSHIP),
		additional(Concepts.ADDITIONAL_RELATIONSHIP);

		final String conceptId;

		CharacteristicType(String conceptId) {
			this.conceptId = conceptId;
		}

		public String getConceptId() {
			return conceptId;
		}

	}
	public interface Fields extends SnomedComponent.Fields {

		String RELATIONSHIP_ID = "relationshipId";
		String SOURCE_ID = "sourceId";
		String DESTINATION_ID = "destinationId";
		String VALUE = "value";
		String RELATIONSHIP_GROUP = "relationshipGroup";
		String TYPE_ID = "typeId";
		String CHARACTERISTIC_TYPE_ID = "characteristicTypeId";
		String MODIFIER_ID = "modifierId";
	}
	@JsonView(value = View.Component.class)
	@Field(type = FieldType.Keyword)
	private String relationshipId;

	@JsonView(value = View.Component.class)
	@Field(type = FieldType.Keyword, store = true)
	private String sourceId;

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	@JsonView(value = View.Component.class)
	@Field(type = FieldType.Keyword)
	@Size(min = 5, max = 18)
	private String destinationId;

	@JsonIgnore
	@JsonView(value = View.Component.class)
	@Field(type = FieldType.Keyword)
	@Size(min = 2, max = 4096)
	private String value;

	@JsonInclude(JsonInclude.Include.NON_EMPTY)
	@JsonView(value = View.Component.class)
	@Transient
	private ConcreteValue concreteValue;

	@Field(type = FieldType.Integer)
	private int relationshipGroup;

	@JsonView(value = View.Component.class)
	@Field(type = FieldType.Keyword)
	@NotNull
	@Size(min = 5, max = 18)
	private String typeId;

	@Field(type = FieldType.Keyword)
	@NotNull
	@Size(min = 5, max = 18)
	private String characteristicTypeId;

	@Field(type = FieldType.Keyword)
	@NotNull
	@Size(min = 5, max = 18)
	private String modifierId;

	@Transient
	private ConceptMini source;

	@Transient
	private ConceptMini type;

	@Transient
	private ConceptMini target;

	@JsonIgnore
	@Transient
	private Short attributeOrder;

	@JsonIgnore
	@Transient
	private Integer groupOrder;

	public Relationship() {
		active = true;
		setModuleId(Concepts.CORE_MODULE);
		destinationId = null;
		typeId = "";
		characteristicTypeId = Concepts.INFERRED_RELATIONSHIP;
		modifierId = Concepts.EXISTENTIAL;
	}

	public Relationship(String relationshipId) {
		this();
		this.relationshipId = relationshipId;
	}

	public Relationship(String typeId, String destinationId) {
		this();
		this.typeId = trim(typeId);
		this.destinationId = trim(destinationId);
	}

	// To trim terms in test data
	private String trim(String typeId) {
		return typeId != null && typeId.contains(" ") ? typeId.substring(0, typeId.indexOf(" ")) : typeId;
	}

	public Relationship(String relationshipId, String typeId, String destinationId) {
		this(relationshipId);
		this.typeId = typeId;
		this.destinationId = destinationId;
	}
	
	public Relationship(String relationshipId, String typeId, ConcreteValue value) {
		this(relationshipId);
		this.typeId = typeId;
		this.value = value.toString();
		this.concreteValue = value;
	}

	public Relationship(String id, Integer effectiveTime, boolean active, String moduleId, String sourceId, String destinationIdOrValue, int relationshipGroup, String typeId, String characteristicTypeId, String modifierId) {
		this();
		this.relationshipId = id;
		setEffectiveTimeI(effectiveTime);
		this.active = active;
		setModuleId(moduleId);
		this.sourceId = sourceId;

		if (destinationIdOrValue != null) {
			if (destinationIdOrValue.startsWith("#") || destinationIdOrValue.startsWith("\"")) {
				this.value = destinationIdOrValue;
				this.destinationId = null;
			} else {
				this.value = null;
				this.destinationId = destinationIdOrValue;
			}
		}

		this.relationshipGroup = relationshipGroup;
		this.typeId = typeId;
		this.characteristicTypeId = characteristicTypeId;
		this.modifierId = modifierId;
	}

	public static Relationship newConcrete(String typeId, ConcreteValue concreteValue) {
		final Relationship relationship = new Relationship();
		relationship.setTypeId(typeId);
		relationship.setConcreteValue(concreteValue);
		return relationship;
	}

	@Override
	public String getIdField() {
		return Fields.RELATIONSHIP_ID;
	}

	@Override
	public boolean isComponentChanged(Relationship that) {
		return that == null
				|| active != that.active
				|| !getModuleId().equals(that.getModuleId())
				|| (destinationId != null && !destinationId.equals(that.destinationId))
				|| (value != null && !value.equals(that.value))
				|| relationshipGroup != that.relationshipGroup
				|| !typeId.equals(that.typeId)
				|| !characteristicTypeId.equals(that.characteristicTypeId)
				|| !modifierId.equals(that.modifierId);
	}
	
	public boolean isGrouped() {
		return relationshipGroup > 0;
	}

	public boolean isInferred() {
		return INFERRED_RELATIONSHIP.equals(characteristicTypeId);
	}

	public ConcreteValue getConcreteValue() {
		if (isConcrete() && this.concreteValue == null && value != null) {
			this.concreteValue = ConcreteValue.from(value);
		}
		return this.concreteValue;
	}

	public void setConcreteValue(ConcreteValue concreteValue) {
		this.concreteValue = concreteValue;
		if (concreteValue != null) {
			this.value = concreteValue.getValueWithPrefix();
			this.destinationId = null;
			this.target = null;
		}
	}

	public void setConcreteValue(String value, String dataTypeName) {
		setConcreteValue(ConcreteValue.from(value, dataTypeName));
		this.value = value;
	}

	public boolean isConcrete() {
		return this.value != null && this.destinationId == null;
	}

	@Override
	protected Object[] getReleaseHashObjects() {
		return new Object[] {active, getModuleId(), destinationId != null ? destinationId : value, relationshipGroup, typeId, characteristicTypeId, modifierId};
	}

	public ConceptMini getSource() {
		return source;
	}

	public ConceptMini getType() {
		return type;
	}

	public ConceptMini getTarget() {
		return target;
	}

	@Override
	@JsonIgnore
	public String getId() {
		return relationshipId;
	}

	@JsonView(value = View.Component.class)
	@JsonProperty("id")
	public String getRelId() {
		return relationshipId;
	}

	@Override
	public Relationship setActive(boolean active) {
		return (Relationship) super.setActive(active);
	}

	@JsonView(value = View.Component.class)
	public ConceptMini source() {
		return source;
	}

	public void setSource(ConceptMini source) {
		this.source = source;
		this.sourceId = source == null ? null : source.getConceptId();
	}

	@JsonView(value = View.Component.class)
	public ConceptMini type() {
		return type;
	}

	public void setType(ConceptMini type) {
		this.type = type;
		if (type != null && !Strings.isNullOrEmpty(type.getConceptId())) {
			this.typeId = type.getConceptId();
		}
	}

	@JsonView(value = View.Component.class)
	public ConceptMini target() {
		return target;
	}

	public Relationship setTarget(ConceptMini target) {
		if (target != null && !Strings.isNullOrEmpty(target.getConceptId())) {
			this.destinationId = target.getConceptId();
		} else {
			target = null;
		}
		this.target = target;
		return this;
	}

	@JsonView(value = View.Component.class)
	public int getGroupId() {
		return relationshipGroup;
	}

	public Relationship setGroupId(int groupId) {
		this.relationshipGroup = groupId;
		return this;
	}

	@JsonView(value = View.Component.class)
	public String getCharacteristicType() {
		return relationshipCharacteristicTypeNames.get(characteristicTypeId);
	}

	public void setCharacteristicType(String characteristicTypeName) {
		characteristicTypeId = relationshipCharacteristicTypeNames.inverse().get(characteristicTypeName);
	}

	public Relationship setCharacteristicTypeId(String characteristicTypeId) {
		this.characteristicTypeId = characteristicTypeId;
		return this;
	}

	public Relationship setInferred(boolean inferred) {
		this.characteristicTypeId = inferred ? Concepts.INFERRED_RELATIONSHIP : Concepts.STATED_RELATIONSHIP;
		return this;
	}

	@JsonView(value = View.Component.class)
	public String getModifier() {
		return relationshipModifierNames.get(modifierId);
	}

	public void setModifier(String modifierName) {
		modifierId = relationshipModifierNames.inverse().get(modifierName);
	}

	public String getRelationshipId() {
		return relationshipId;
	}

	public Long getRelationshipIdAsLong() {
		return relationshipId != null ? Long.parseLong(relationshipId) : null;
	}

	public void setRelationshipId(String relationshipId) {
		this.relationshipId = relationshipId;
	}

	public String getSourceId() {
		return sourceId;
	}

	public Relationship setSourceId(String sourceId) {
		this.sourceId = sourceId;
		return this;
	}

	public String getDestinationId() {
		return destinationId;
	}

	public void setDestinationId(String destinationId) {
		this.value = null;
		this.destinationId = destinationId;
	}

	public String getValue() {
		return value;
	}

	@JsonIgnore
	public String getValueWithoutConcretePrefix() {
		return ConcreteValue.removeConcretePrefix(this.value);
	}

	public void setValue(String value) {
		this.destinationId = null;
		this.value = value;
	}

	public int getRelationshipGroup() {
		return relationshipGroup;
	}

	public void setRelationshipGroup(int relationshipGroup) {
		this.relationshipGroup = relationshipGroup;
	}

	public String getTypeId() {
		return typeId;
	}

	public Relationship setTypeId(String typeId) {
		this.typeId = typeId;
		return this;
	}

	public String getCharacteristicTypeId() {
		return characteristicTypeId;
	}

	public String getModifierId() {
		return modifierId;
	}

	@JsonIgnore
	public Short getAttributeOrder() {
		return attributeOrder;
	}

	public void setAttributeOrder(Short attributeOrder) {
		this.attributeOrder = attributeOrder;
	}

	@JsonIgnore
	public int getGroupOrder() {
		return groupOrder != null ? groupOrder : relationshipGroup;
	}

	public void setGroupOrder(int groupOrder) {
		this.groupOrder = groupOrder;
	}

	@JsonIgnore
	public String getTargetFsn() {
		return target != null ? target.getFsnTerm() : null;
	}

	public void clone(Relationship relationship) {
		setRelationshipId(relationship.getRelationshipId());
		setEffectiveTimeI(relationship.getEffectiveTimeI());
		setReleasedEffectiveTime(relationship.getReleasedEffectiveTime());
		setReleaseHash(relationship.getReleaseHash());
		setReleased(relationship.isReleased());
		setActive(relationship.isActive());
		setModuleId(relationship.getModuleId());
		setSourceId(relationship.getSourceId());

		if (relationship.getDestinationId() != null) {
			setDestinationId(relationship.getDestinationId());
			setTarget(relationship.getTarget());
		} else {
			setValue(relationship.getValue());
		}

		setRelationshipGroup(relationship.getRelationshipGroup());
		setTypeId(relationship.getTypeId());
		setType(relationship.getType());
		setCharacteristicTypeId(relationship.getCharacteristicTypeId());
		setModifier(relationship.getModifier());

		updateEffectiveTime();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Relationship that = (Relationship) o;

		if (relationshipId != null && relationshipId.equals(that.relationshipId)) {
			return true;
		}

		if (relationshipGroup != that.relationshipGroup) return false;
		if (!Objects.equals(sourceId, that.sourceId)) return false;
		if (!Objects.equals(destinationId, that.destinationId)) return false;
		if (!Objects.equals(value, that.value)) return false;
		if (!Objects.equals(typeId, that.typeId)) return false;
		return Objects.equals(characteristicTypeId, that.characteristicTypeId);
	}

	@Override
	public int hashCode() {
		int result = relationshipId != null ? relationshipId.hashCode() : 0;
		if (result != 0) {
			return result;
		}
		result = 31 * result + (sourceId != null ? sourceId.hashCode() : 0);
		result = 31 * result + (destinationId != null ? destinationId.hashCode() : 0);
		result = 31 * result + (value != null ? value.hashCode() : 0);
		result = 31 * result + relationshipGroup;
		result = 31 * result + (typeId != null ? typeId.hashCode() : 0);
		result = 31 * result + (characteristicTypeId != null ? characteristicTypeId.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "Relationship{" +
				"relationshipId='" + relationshipId + '\'' +
				", effectiveTime='" + getEffectiveTimeI() + '\'' +
				", active=" + active +
				", moduleId='" + getModuleId() + '\'' +
				", sourceId='" + sourceId + '\'' +
				", destinationId='" + destinationId + '\'' +
				", value='" + value + '\'' +
				", relationshipGroup='" + relationshipGroup + '\'' +
				", typeId='" + typeId + '\'' +
				", characteristicTypeId='" + characteristicTypeId + '\'' +
				", modifierId='" + modifierId + '\'' +
				", internalId='" + getInternalId() + '\'' +
				", start='" + getStart() + '\'' +
				", end='" + getEnd() + '\'' +
				", path='" + getPath() + '\'' +
				'}';
	}
}

INSERT INTO mimic100_synthetic.concept (
    concept_class_id,
    concept_code,
    concept_id,
    concept_name,
    domain_id,
    invalid_reason,
    standard_concept,
    valid_end_date,
    valid_start_date,
    vocabulary_id
)
SELECT
    concept_class_id,
    concept_code,
    concept_id,
    concept_name,
    domain_id,
    invalid_reason,
    standard_concept,
    valid_end_date,
    valid_start_date,
    vocabulary_id
FROM mimic100.concept;

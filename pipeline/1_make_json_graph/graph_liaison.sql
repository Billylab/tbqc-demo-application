WITH dataset_1_clinical_trials AS (
SELECT 
    atcode,
    drug,
    ARRAY_AGG(
    STRUCT(ct.id,ct.scientific_title,ct.date,ct.journal)
    ) as clinical_trials
FROM `test01-lincoln-project.1_raw.drugs` as d
CROSS JOIN `test01-lincoln-project.1_raw.clinical_trials` as ct
WHERE REGEXP_CONTAINS(UPPER(ct.scientific_title), UPPER(drug))
GROUP BY atcode, drug
ORDER BY atcode
),

dataset_1_clinical_trials_journal AS (
SELECT 
    atcode,
    drug,
    ct.id,
    ct.scientific_title,
    ct.date,
    ct.journal
FROM `test01-lincoln-project.1_raw.drugs` as d
CROSS JOIN `test01-lincoln-project.1_raw.clinical_trials` as ct
WHERE REGEXP_CONTAINS(UPPER(ct.scientific_title), UPPER(drug))
ORDER BY atcode
),

dataset_2_pubmed AS (
SELECT 
    atcode,
    drug,
    ARRAY_AGG(
    STRUCT(p.id,p.title,p.date,p.journal)
    ) as pubmed
FROM `test01-lincoln-project.1_raw.drugs` as d
CROSS JOIN `test01-lincoln-project.1_raw.pubmed` as p
WHERE REGEXP_CONTAINS(UPPER(p.title), UPPER(drug))
GROUP BY atcode, drug
ORDER BY atcode
),

dataset_2_pubmed_journal AS (
SELECT 
    atcode,
    drug,
    p.id,
    p.title,
    p.date,
    p.journal
FROM `test01-lincoln-project.1_raw.drugs` as d
CROSS JOIN `test01-lincoln-project.1_raw.pubmed` as p
WHERE REGEXP_CONTAINS(UPPER(p.title), UPPER(drug))
ORDER BY atcode
),

dataset3 AS (
SELECT DISTINCT *
FROM (
    SELECT
    d.atcode,
    d.drug,
    d1.journal,
    d1.date,
    FROM `test01-lincoln-project.1_raw.drugs` as d
    LEFT JOIN dataset_1_clinical_trials_journal as d1 ON d.atcode = d1.atcode
    UNION ALL
    SELECT
    d.atcode,
    d.drug,
    d2.journal,
    d2.date,
    FROM `test01-lincoln-project.1_raw.drugs` as d
    LEFT JOIN dataset_2_pubmed_journal as d2 ON d.atcode = d2.atcode
)
WHERE journal IS NOT NULL AND date IS NOT NULL
),

dataset4 AS (
SELECT
    atcode,
    drug,
    ARRAY_AGG(
    STRUCT(date,journal)
    ) as journal
    FROM dataset3
    GROUP BY atcode, drug
)

SELECT
d.atcode,
d.drug,
d1.clinical_trials,
d2.pubmed,
d4.journal
FROM `test01-lincoln-project.1_raw.drugs` as d
LEFT JOIN dataset_1_clinical_trials as d1 ON d.atcode = d1.atcode
LEFT JOIN dataset_2_pubmed as d2 ON d.atcode = d2.atcode
LEFT JOIN dataset4 as d4 ON d.atcode = d4.atcode
ORDER BY atcode;
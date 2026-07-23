import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F, Row

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = args['S3_BUCKET']
BRONZE = f"s3://{BUCKET}/bronze/filings/"
SILVER = f"s3://{BUCKET}/silver/filings/"

print(f"Starting SEC Filings ETL — bucket: {BUCKET}")

SEVERITY_MAP = {
    'EARNINGS_RELEASE': 4, 'ASSET_ACQUISITION': 5,
    'MATERIAL_AGREEMENT': 3, 'EXECUTIVE_CHANGE': 4,
    'RESTRUCTURING_LAYOFFS': 5, 'BANKRUPTCY': 5,
    'AGREEMENT_TERMINATION': 3, 'ASSET_IMPAIRMENT': 4,
    'EXCHANGE_DELISTING': 5, 'AUDITOR_CHANGE': 4,
    'CHARTER_AMENDMENT': 2, 'REGULATION_FD': 2,
    'FINANCIAL_STATEMENTS': 3, 'OTHER_MATERIAL_EVENT': 2,
    'OTHER': 1, 'UNKNOWN': 1
}

SENTIMENT_MAP = {
    'EARNINGS_RELEASE': 'WATCH', 'ASSET_ACQUISITION': 'BULLISH',
    'MATERIAL_AGREEMENT': 'BULLISH', 'EXECUTIVE_CHANGE': 'BEARISH',
    'RESTRUCTURING_LAYOFFS': 'BEARISH', 'BANKRUPTCY': 'CRITICAL',
    'AGREEMENT_TERMINATION': 'BEARISH', 'ASSET_IMPAIRMENT': 'BEARISH',
    'EXCHANGE_DELISTING': 'CRITICAL', 'AUDITOR_CHANGE': 'BEARISH',
    'CHARTER_AMENDMENT': 'NEUTRAL', 'REGULATION_FD': 'NEUTRAL',
    'FINANCIAL_STATEMENTS': 'WATCH', 'OTHER_MATERIAL_EVENT': 'WATCH',
    'OTHER': 'NEUTRAL', 'UNKNOWN': 'NEUTRAL'
}

SECTOR_MAP = {
    'EARNINGS_RELEASE': 'ALL_SECTORS',
    'ASSET_ACQUISITION': 'FINANCE_STRATEGY',
    'MATERIAL_AGREEMENT': 'LEGAL_STRATEGY',
    'EXECUTIVE_CHANGE': 'GOVERNANCE',
    'RESTRUCTURING_LAYOFFS': 'OPERATIONS_HR',
    'BANKRUPTCY': 'FINANCE_LEGAL',
    'AGREEMENT_TERMINATION': 'LEGAL_STRATEGY',
    'ASSET_IMPAIRMENT': 'FINANCE_ACCOUNTING',
    'EXCHANGE_DELISTING': 'FINANCE_COMPLIANCE',
    'AUDITOR_CHANGE': 'FINANCE_COMPLIANCE',
    'CHARTER_AMENDMENT': 'GOVERNANCE_LEGAL',
    'REGULATION_FD': 'INVESTOR_RELATIONS',
    'FINANCIAL_STATEMENTS': 'FINANCE_ACCOUNTING',
    'OTHER_MATERIAL_EVENT': 'GENERAL',
    'OTHER': 'GENERAL', 'UNKNOWN': 'GENERAL'
}

# ── Read + parse using wholeTextFiles ─────────────────────────────────────────
raw_rdd = sc.wholeTextFiles(f"s3://{BUCKET}/bronze/filings/*/*.json")

def parse_filing(file_tuple):
    _, content = file_tuple
    try:
        d = json.loads(content)
        et = d.get('event_type', 'UNKNOWN')
        filed = d.get('filed_date', '2026-01-01')[:10]
        parts = filed.split('-') if filed and len(filed) == 10 else ['2026','7','1']
        return [Row(
            filing_id           = str(d.get('filing_id', '')),
            company_name        = str(d.get('company_name', '')),
            cik                 = str(d.get('cik', '')),
            form_type           = str(d.get('form_type', '8-K')),
            filed_date          = str(d.get('filed_date', '')),
            filed_datetime      = str(d.get('filed_datetime', '')),
            accession_number    = str(d.get('accession_number', '')),
            filing_url          = str(d.get('filing_url', '')),
            item_code           = str(d.get('item_code', 'UNKNOWN')),
            event_type          = str(et),
            ingested_at         = str(d.get('ingested_at', '')),
            severity_score      = int(SEVERITY_MAP.get(et, 1)),
            preliminary_sentiment = str(SENTIMENT_MAP.get(et, 'NEUTRAL')),
            sector_relevance    = str(SECTOR_MAP.get(et, 'GENERAL')),
            is_high_severity    = bool(SEVERITY_MAP.get(et, 1) >= 4),
            is_market_moving    = bool(SENTIMENT_MAP.get(et, 'NEUTRAL') in ['BEARISH','BULLISH','CRITICAL']),
            etl_version         = 'glue-etl-v1',
            filing_year         = int(parts[0]),
            filing_month        = int(parts[1])
        )]
    except Exception as e:
        print(f"Parse error: {e}")
        return []

rows_rdd = raw_rdd.flatMap(parse_filing)
print(f"Parsed rows: {rows_rdd.count()}")

# ── Create DataFrame from typed Row objects ───────────────────────────────────
enriched_df = spark.createDataFrame(rows_rdd)
print(f"DataFrame rows: {enriched_df.count()}")
enriched_df.printSchema()

# ── Summary stats ─────────────────────────────────────────────────────────────
print("\n=== Event Type Distribution ===")
enriched_df.groupBy("event_type", "preliminary_sentiment", "severity_score") \
    .count() \
    .orderBy(F.desc("severity_score"), F.desc("count")) \
    .show(20, truncate=False)

print("\n=== High Severity Filings ===")
enriched_df.filter(F.col("is_high_severity") == True) \
    .select("company_name", "event_type", "severity_score",
            "preliminary_sentiment", "filed_date") \
    .orderBy(F.desc("severity_score")) \
    .show(20, truncate=False)

# ── Write to Silver ───────────────────────────────────────────────────────────
enriched_df.write \
    .mode("overwrite") \
    .partitionBy("filing_year", "filing_month") \
    .parquet(SILVER)

print("✓ Silver zone written")
job.commit()
print("✓ ETL complete")

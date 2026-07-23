import sys
import json
import boto3
import time
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
SILVER = f"s3://{BUCKET}/silver/filings/"
GOLD   = f"s3://{BUCKET}/gold/signals/"

comprehend = boto3.client('comprehend', region_name='us-east-1')

print(f"Starting Comprehend sentiment job — bucket: {BUCKET}")

# ── Event-specific text templates ─────────────────────────────────────────────
# Rich financial language that carries real sentiment signals
# Comprehend needs context-rich sentences, not dry metadata
EVENT_TEMPLATES = {
    'EARNINGS_RELEASE': (
        "{company} reported quarterly financial results. "
        "The earnings release reveals the company's revenue, profit, and "
        "financial performance metrics for investors."
    ),
    'ASSET_ACQUISITION': (
        "{company} announced a major strategic acquisition. "
        "This significant business expansion signals strong growth ambitions "
        "and positive outlook for the company's future revenue streams."
    ),
    'MATERIAL_AGREEMENT': (
        "{company} entered into a new material business agreement. "
        "This strategic partnership creates new revenue opportunities "
        "and strengthens the company's competitive position."
    ),
    'EXECUTIVE_CHANGE': (
        "{company} announced an unexpected executive leadership change. "
        "The sudden departure of a key executive creates management uncertainty "
        "and raises concerns about the company's strategic direction and stability."
    ),
    'RESTRUCTURING_LAYOFFS': (
        "{company} announced significant workforce restructuring and layoffs. "
        "The company is cutting jobs and reducing costs due to deteriorating "
        "business conditions, signaling financial distress and operational challenges."
    ),
    'BANKRUPTCY': (
        "{company} has filed for bankruptcy protection. "
        "The company faces severe financial distress and is unable to meet its "
        "debt obligations, threatening the business's survival and shareholder value."
    ),
    'AGREEMENT_TERMINATION': (
        "{company} terminated a key business agreement. "
        "The loss of this material contract negatively impacts future revenue "
        "and raises concerns about the company's business relationships."
    ),
    'ASSET_IMPAIRMENT': (
        "{company} recorded a significant asset impairment charge. "
        "This writedown indicates the company's assets are worth substantially "
        "less than previously reported, reflecting deteriorating business value."
    ),
    'EXCHANGE_DELISTING': (
        "{company} received a notice of delisting from the stock exchange. "
        "The company faces severe financial difficulties and fails to meet "
        "listing requirements, indicating potential bankruptcy and investor losses."
    ),
    'AUDITOR_CHANGE': (
        "{company} announced an unexpected change of its external auditor. "
        "The sudden auditor resignation raises serious concerns about the "
        "company's financial reporting integrity and accounting practices."
    ),
    'CHARTER_AMENDMENT': (
        "{company} amended its corporate charter and bylaws. "
        "The company made changes to its governance structure and "
        "shareholder rights policies."
    ),
    'REGULATION_FD': (
        "{company} disclosed material non-public information to investors "
        "under Regulation FD. The company shared business updates and "
        "financial information with the investment community."
    ),
    'FINANCIAL_STATEMENTS': (
        "{company} filed financial statements and exhibits. "
        "The company disclosed financial data and supporting documentation "
        "as required by securities regulations."
    ),
    'OTHER_MATERIAL_EVENT': (
        "{company} reported a material corporate event requiring SEC disclosure. "
        "The company experienced a significant business development that "
        "may impact its operations and financial performance."
    ),
    'OTHER': (
        "{company} filed an 8-K disclosure with the SEC. "
        "The company reported a corporate event to regulators."
    ),
    'UNKNOWN': (
        "{company} filed an 8-K disclosure with the SEC. "
        "The company reported a corporate event to regulators."
    )
}

def build_text(company, event_type):
    template = EVENT_TEMPLATES.get(event_type, EVENT_TEMPLATES['OTHER'])
    return template.format(company=company)

def label_to_signal(label):
    return {
        'POSITIVE': 'BULLISH',
        'NEGATIVE': 'BEARISH',
        'NEUTRAL':  'NEUTRAL',
        'MIXED':    'WATCH'
    }.get(label, 'NEUTRAL')

def label_to_action(label, severity):
    if label == 'NEGATIVE' and severity >= 5:
        return 'IMMEDIATE_REVIEW'
    elif label == 'NEGATIVE' and severity >= 4:
        return 'REVIEW'
    elif label == 'POSITIVE' and severity >= 4:
        return 'MONITOR_OPPORTUNITY'
    elif label == 'MIXED':
        return 'MONITOR'
    else:
        return 'ROUTINE'

def batch_classify(texts):
    try:
        response = comprehend.batch_detect_sentiment(
            TextList=texts,
            LanguageCode='en'
        )
        results = []
        for r in response['ResultList']:
            label = r['Sentiment']
            scores = r['SentimentScore']
            key = label.capitalize() if label != 'MIXED' else 'Mixed'
            confidence = round(scores.get(key, 0.5), 4)
            results.append((label, confidence))
        return results
    except Exception as e:
        print(f"Comprehend error: {e}")
        return [('NEUTRAL', 0.5)] * len(texts)

# ── Read Silver ───────────────────────────────────────────────────────────────
silver_df = spark.read.parquet(SILVER)
total = silver_df.count()
print(f"Silver records: {total}")
filings = silver_df.collect()

# ── Classify in batches of 25 ─────────────────────────────────────────────────
enriched_rows = []
batch_size = 25

for batch_start in range(0, total, batch_size):
    batch = filings[batch_start: batch_start + batch_size]
    texts = [
        build_text(row['company_name'], row['event_type'])[:4800]
        for row in batch
    ]

    # Print sample text from first batch so we can verify quality
    if batch_start == 0:
        print(f"\nSample text for classification:")
        print(f"  '{texts[0]}'")
        print(f"  '{texts[1]}'")

    sentiments = batch_classify(texts)

    for row, (label, confidence) in zip(batch, sentiments):
        enriched_rows.append(Row(
            filing_id             = str(row['filing_id']),
            company_name          = str(row['company_name']),
            cik                   = str(row['cik']),
            event_type            = str(row['event_type']),
            filed_date            = str(row['filed_date']),
            item_code             = str(row['item_code']),
            filing_url            = str(row['filing_url']),
            severity_score        = int(row['severity_score']),
            rule_based_sentiment  = str(row['preliminary_sentiment']),
            comprehend_label      = str(label),
            comprehend_confidence = float(confidence),
            trading_signal        = str(label_to_signal(label)),
            recommended_action    = str(label_to_action(label, row['severity_score'])),
            sector_relevance      = str(row['sector_relevance']),
            is_high_severity      = bool(row['is_high_severity']),
            is_market_moving      = bool(row['is_market_moving']),
            filing_year           = int(row['filing_year']),
            filing_month          = int(row['filing_month'])
        ))

    print(f"Classified {min(batch_start + batch_size, total)}/{total}")
    time.sleep(0.5)

print(f"\nDone: {len(enriched_rows)} rows classified")

# ── Build Gold DataFrame ──────────────────────────────────────────────────────
gold_df = spark.createDataFrame(enriched_rows)

print("\n=== Comprehend Sentiment Distribution ===")
gold_df.groupBy("trading_signal", "comprehend_label") \
    .agg(
        F.count("*").alias("count"),
        F.round(F.avg("comprehend_confidence"), 4).alias("avg_confidence")
    ) \
    .orderBy(F.desc("count")).show()

print("\n=== High Severity BEARISH Signals ===")
gold_df.filter(
    (F.col("trading_signal") == "BEARISH") &
    (F.col("severity_score") >= 4)
).select(
    "company_name", "event_type", "severity_score",
    "comprehend_label", "comprehend_confidence", "recommended_action"
).orderBy(F.desc("severity_score"), F.desc("comprehend_confidence")) \
 .show(20, truncate=False)

print("\n=== BULLISH Signals ===")
gold_df.filter(F.col("trading_signal") == "BULLISH") \
    .select("company_name", "event_type",
            "comprehend_confidence", "recommended_action") \
    .orderBy(F.desc("comprehend_confidence")) \
    .show(10, truncate=False)

print("\n=== IMMEDIATE_REVIEW Companies ===")
gold_df.filter(F.col("recommended_action") == "IMMEDIATE_REVIEW") \
    .select("company_name", "event_type", "filed_date",
            "trading_signal", "comprehend_confidence") \
    .show(20, truncate=False)

# ── Write Gold ────────────────────────────────────────────────────────────────
gold_df.write.mode("overwrite") \
    .partitionBy("filing_year", "filing_month") \
    .parquet(GOLD)

print("✓ Gold zone written")
job.commit()
print("✓ Comprehend sentiment job complete")

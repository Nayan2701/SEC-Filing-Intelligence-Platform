import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SEC Filing Intelligence Platform",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    try:
        user = os.environ["SNOWFLAKE_USER"]
        password = os.environ["SNOWFLAKE_PASSWORD"]
        account = os.environ["SNOWFLAKE_ACCOUNT"]
    except KeyError:
        user = st.secrets["snowflake"]["user"]
        password = st.secrets["snowflake"]["password"]
        account = st.secrets["snowflake"]["account"]
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse="SEC_WH",
        database="SEC_INTELLIGENCE",
        schema="ANALYTICS",
        role="ACCOUNTADMIN"
    )

@st.cache_data(ttl=300)
def run_query(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    return pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("📈 SEC Filing Intelligence")
st.sidebar.markdown("""
**Real-time SEC 8-K Filing Analytics**

Built on:
- AWS Kinesis + Lambda
- AWS Glue + Comprehend NLP
- Snowflake Analytics
- EventBridge (10-min ingestion)

*Aligned with SEC EDGAR real-time feed*
""")

page = st.sidebar.radio(
    "Navigate",
    ["📊 Intelligence Feed",
     "⚠️ Watchlist",
     "📡 Signal Summary",
     "🏭 Sector Intelligence"]
)

# ── Page 1: Intelligence Feed ─────────────────────────────────────────────────
if page == "📊 Intelligence Feed":
    st.title("📊 SEC 8-K Filing Intelligence Feed")
    st.markdown(
        "Real-time SEC 8-K filings enriched with AWS Comprehend NLP sentiment — "
        "updated every 10 minutes via EventBridge + Lambda."
    )

    df = run_query("SELECT * FROM VW_FILING_INTELLIGENCE")

    # KPI row
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Filings", f"{len(df):,}")
    col2.metric("BEARISH Signals",
                f"{(df['TRADING_SIGNAL'] == 'BEARISH').sum()}")
    col3.metric("BULLISH Signals",
                f"{(df['TRADING_SIGNAL'] == 'BULLISH').sum()}")
    col4.metric("Immediate Review",
                f"{(df['RECOMMENDED_ACTION'] == 'IMMEDIATE_REVIEW').sum()}")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Signal Distribution")
        signal_counts = df['TRADING_SIGNAL'].value_counts().reset_index()
        signal_counts.columns = ['Signal', 'Count']
        fig = px.pie(
            signal_counts,
            values='Count',
            names='Signal',
            color='Signal',
            color_discrete_map={
                'BULLISH': '#2ecc71',
                'BEARISH': '#e74c3c',
                'NEUTRAL': '#95a5a6',
                'WATCH': '#f39c12'
            },
            hole=0.4
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Event Type Breakdown")
        event_counts = df['EVENT_TYPE'].value_counts().head(10).reset_index()
        event_counts.columns = ['Event Type', 'Count']
        fig = px.bar(
            event_counts,
            x='Count',
            y='Event Type',
            orientation='h',
            color='Count',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Live Filing Feed")
    signal_filter = st.multiselect(
        "Filter by Signal",
        options=['BULLISH', 'BEARISH', 'NEUTRAL', 'WATCH'],
        default=['BULLISH', 'BEARISH']
    )
    filtered = df[df['TRADING_SIGNAL'].isin(signal_filter)] if signal_filter else df
    st.dataframe(
        filtered[[
            'COMPANY_NAME', 'EVENT_TYPE', 'FILED_DATE',
            'TRADING_SIGNAL', 'CONFIDENCE_PCT',
            'SEVERITY_SCORE', 'RECOMMENDED_ACTION'
        ]].style.format({'CONFIDENCE_PCT': '{:.1f}%'}),
        use_container_width=True,
        height=400
    )

# ── Page 2: Watchlist ─────────────────────────────────────────────────────────
elif page == "⚠️ Watchlist":
    st.title("⚠️ Company Watchlist")
    st.markdown(
        "High severity filings and bearish signals requiring immediate attention. "
        "Updated in real-time as new 8-K filings appear on EDGAR."
    )

    df = run_query("SELECT * FROM VW_WATCHLIST")

    col1, col2, col3 = st.columns(3)
    col1.metric("Companies on Watchlist", f"{len(df):,}")
    col2.metric("Immediate Review",
                f"{(df['RECOMMENDED_ACTION'] == 'IMMEDIATE_REVIEW').sum()}")
    col3.metric("Avg Severity Score",
                f"{df['SEVERITY_SCORE'].mean():.1f}/5")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Severity Distribution")
        sev_counts = df['SEVERITY_SCORE'].value_counts().sort_index().reset_index()
        sev_counts.columns = ['Severity', 'Count']
        fig = px.bar(
            sev_counts,
            x='Severity',
            y='Count',
            color='Severity',
            color_continuous_scale='Reds'
        )
        fig.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Action Required")
        action_counts = df['RECOMMENDED_ACTION'].value_counts().reset_index()
        action_counts.columns = ['Action', 'Count']
        fig = px.pie(
            action_counts,
            values='Count',
            names='Action',
            color_discrete_sequence=px.colors.sequential.RdBu
        )
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Watchlist Companies")
    st.dataframe(
        df[[
            'COMPANY_NAME', 'EVENT_TYPE', 'FILED_DATE',
            'TRADING_SIGNAL', 'CONFIDENCE_PCT',
            'SEVERITY_SCORE', 'RECOMMENDED_ACTION', 'FILING_URL'
        ]].style.format({'CONFIDENCE_PCT': '{:.1f}%'}),
        use_container_width=True
    )

# ── Page 3: Signal Summary ────────────────────────────────────────────────────
elif page == "📡 Signal Summary":
    st.title("📡 Signal Summary")
    st.markdown("Aggregated trading signals and recommended actions across all filings.")

    df = run_query("SELECT * FROM VW_SIGNAL_SUMMARY")

    st.subheader("Signal × Action Matrix")
    fig = px.treemap(
        df,
        path=['TRADING_SIGNAL', 'RECOMMENDED_ACTION'],
        values='FILING_COUNT',
        color='AVG_CONFIDENCE_PCT',
        color_continuous_scale='RdYlGn',
        labels={'FILING_COUNT': 'Filings', 'AVG_CONFIDENCE_PCT': 'Avg Confidence %'}
    )
    fig.update_layout(height=450)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Detailed Signal Breakdown")
    st.dataframe(
        df.style.format({
            'AVG_CONFIDENCE_PCT': '{:.1f}%',
            'AVG_SEVERITY': '{:.2f}'
        }),
        use_container_width=True
    )

# ── Page 4: Sector Intelligence ───────────────────────────────────────────────
elif page == "🏭 Sector Intelligence":
    st.title("🏭 Sector Intelligence")
    st.markdown(
        "Filing patterns and sentiment signals by business sector. "
        "Identifies which sectors are experiencing the most material events."
    )

    df = run_query("SELECT * FROM VW_SECTOR_INTELLIGENCE")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Filings by Sector")
        sector_total = df.groupby('SECTOR_RELEVANCE')['FILING_COUNT'].sum().reset_index()
        sector_total.columns = ['Sector', 'Count']
        fig = px.bar(
            sector_total.sort_values('Count', ascending=True),
            x='Count',
            y='Sector',
            orientation='h',
            color='Count',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Sentiment by Sector")
        fig = px.bar(
            df,
            x='SECTOR_RELEVANCE',
            y='FILING_COUNT',
            color='TRADING_SIGNAL',
            color_discrete_map={
                'BULLISH': '#2ecc71',
                'BEARISH': '#e74c3c',
                'NEUTRAL': '#95a5a6',
                'WATCH': '#f39c12'
            },
            barmode='stack'
        )
        fig.update_layout(
            height=400,
            xaxis_tickangle=-45
        )
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Sector Detail Table")
    st.dataframe(
        df.style.format({
            'AVG_SEVERITY': '{:.2f}',
            'AVG_CONFIDENCE_PCT': '{:.1f}%'
        }),
        use_container_width=True
    )

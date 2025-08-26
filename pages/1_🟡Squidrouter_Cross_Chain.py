import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import networkx as nx

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Squidrouter: Cross Chain & School NFT Dashboard",
    page_icon="https://img.cryptorank.io/coins/squid1675241862798.png",
    layout="wide"
)

# --- Title -----------------------------------------------------------------------------------------------------
st.title("🟡Squidrouter Cross Chain")

# --- Builder Info ---------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" style="width:25px; height:25px; border-radius: 50%;">
            <span>Built by: <a href="https://x.com/0xeman_raz" target="_blank">Eman Raz</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs ---------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["month", "week", "day"])

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2023-01-01"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-08-31"))
# --- Query Function: Row1 --------------------------------------------------------------------------------------
@st.cache_data
def load_kpi_data(timeframe, start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_kpi = load_kpi_data(timeframe, start_date, end_date)

# --- KPI Row ------------------------------------------------------------------------------------------------------
logo_url = "https://img.cryptorank.io/coins/squid1675241862798.png"


kpi_card_style = f"""
<style>
.kpi-card {{
    position: relative;
    background-color: white;
    border-radius: 15px;
    padding: 20px;
    text-align: center;
    box-shadow: 0 4px 10px rgba(0,0,0,0.1);
    font-family: Arial, sans-serif;
    overflow: hidden;
    height: 140px;
}}
.kpi-card::before {{
    content: "";
    background: url({logo_url}) no-repeat center;
    background-size: 120px;
    opacity: 0.08;  
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: 160px;
    height: 160px;
    z-index: 0;
}}
.kpi-label {{
    font-size: 18px;
    font-weight: bold;
    color: #333;
    position: relative;
    z-index: 1;
}}
.kpi-value {{
    font-size: 28px;
    font-weight: 700;
    color: #111;
    margin-top: 15px;
    position: relative;
    z-index: 1;
}}
</style>
"""

st.markdown(kpi_card_style, unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">💸Volume of Swaps</div>
        <div class="kpi-value">${df_kpi['VOLUME_OF_TRANSFERS'][0]:,}</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">🚀Number of Swaps</div>
        <div class="kpi-value">{df_kpi['NUMBER_OF_TRANSFERS'][0]:,} Txns</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-label">👨‍💻Number of Swappers</div>
        <div class="kpi-value">{df_kpi['NUMBER_OF_USERS'][0]:,} Addresses</div>
    </div>
    """, unsafe_allow_html=True)
# --- Query Function: Row (2) --------------------------------------------------------------------------------------
@st.cache_data
def load_time_series_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT 
        DATE_TRUNC('{timeframe}', created_at) AS Date,
        COUNT(DISTINCT id) AS Number_of_Transfers, 
        COUNT(DISTINCT user) AS Number_of_Users, 
        ROUND(SUM(amount_usd)) AS Volume_of_Transfers
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' 
      AND created_at::date <= '{end_str}'
    GROUP BY 1
    ORDER BY 1
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_ts = load_time_series_data(timeframe, start_date, end_date)

# --- Charts in One Row ---------------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

with col1:
    fig1 = px.bar(
        df_ts,
        x="DATE",
        y="VOLUME_OF_TRANSFERS",
        title="Swap Volume Over Time ($USD)",
        labels={"VOLUME_OF_TRANSFERS": "Volume (USD)", "DATE": "Date"},
        color_discrete_sequence=["#80b4e4"]
    )
    fig1.update_layout(xaxis_title="", yaxis_title="USD", bargap=0.2)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(
        df_ts,
        x="DATE",
        y="NUMBER_OF_TRANSFERS",
        title="Swap Count Over Time",
        labels={"NUMBER_OF_TRANSFERS": "Transactions", "DATE": "Date"},
        color_discrete_sequence=["#80b4e4"]
    )
    fig2.update_layout(xaxis_title="", yaxis_title="Txns", bargap=0.2)
    st.plotly_chart(fig2, use_container_width=True)

# --- Row 3 ---------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_user_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH table1 AS (
        WITH overview AS (
            WITH axelar_service AS (
                SELECT 
                    created_at, 
                    recipient_address AS user
                FROM axelar.axelscan.fact_transfers
                WHERE status = 'executed'
                  AND simplified_status = 'received'
                  AND (
                      sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
                      OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
                      OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
                      OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
                      OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
                  ) 
                UNION ALL
                SELECT  
                    created_at,
                    data:call.transaction.from::STRING AS user
                FROM axelar.axelscan.fact_gmp 
                WHERE status = 'executed'
                  AND simplified_status = 'received'
                  AND (
                      data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
                      OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
                      OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
                      OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
                      OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
                  ) 
            )
            SELECT user, MIN(created_at::date) AS first_date
            FROM axelar_service
            GROUP BY 1
        )
        SELECT date_trunc('{timeframe}', first_date) AS "Date",
               COUNT(DISTINCT user) AS "New Users", 
               SUM(COUNT(DISTINCT user)) OVER (ORDER BY date_trunc('{timeframe}', first_date)) AS "User Growth"
        FROM overview 
        WHERE first_date >= '{start_str}' AND first_date <= '{end_str}'
        GROUP BY 1
    ),
    table2 AS (
        WITH axelar_service AS (
            SELECT 
                created_at, 
                recipient_address AS user
            FROM axelar.axelscan.fact_transfers
            WHERE status = 'executed'
              AND simplified_status = 'received'
              AND (
                  sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
                  OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
                  OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
                  OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
                  OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
              ) 
            UNION ALL
            SELECT  
                created_at,
                data:call.transaction.from::STRING AS user
            FROM axelar.axelscan.fact_gmp 
            WHERE status = 'executed'
              AND simplified_status = 'received'
              AND (
                  data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
                  OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
                  OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
                  OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
                  OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
              ) 
        )
        SELECT date_trunc('{timeframe}', created_at) AS "Date",
               COUNT(DISTINCT user) AS "Total Users"
        FROM axelar_service
        WHERE created_at::date >= '{start_str}' AND created_at::date <= '{end_str}'
        GROUP BY 1
    )
    SELECT 
        table1."Date" AS "Date",
        "Total Users",
        "New Users", 
        "Total Users" - "New Users" AS "Active Users",
        "User Growth"
    FROM table1 
    LEFT JOIN table2 ON table1."Date" = table2."Date"
    ORDER BY 1;
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data -------------------------------------------------------------------------------------------------
df = load_user_data(timeframe, start_date, end_date)

# --- Charts in One Row ----------------------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Chart 1
with col1:
    fig1 = go.Figure()
    fig1.add_trace(go.Bar(x=df["Date"], y=df["Active Users"], name="Active Users"))
    fig1.add_trace(go.Bar(x=df["Date"], y=df["New Users"], name="New Users"))
    fig1.add_trace(go.Scatter(x=df["Date"], y=df["Total Users"], name="Total Users",
                              mode="lines+markers", line=dict(width=3)))
    fig1.update_layout(
        barmode="stack",
        title="Number of Swappers Over Time",
        xaxis_title=" ",
        yaxis_title="User count",
        legend_title=" ",
        template="plotly_white"
    )
    st.plotly_chart(fig1, use_container_width=True)

# Chart 2
with col2:
    fig2 = px.area(df, x="Date", y="User Growth", title="Users Growth Over Time")
    fig2.update_layout(
        xaxis_title=" ",
        yaxis_title="user count",
        template="plotly_white"
    )
    st.plotly_chart(fig2, use_container_width=True)


# --- Cached Data Loader --------------------------------------------------------------------------------------
@st.cache_data
def load_chain_flows(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS transfer_user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_OBJECT(data:send:amount) 
                   OR IS_ARRAY(data:link:price) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL 
                   AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
              THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            id
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
              sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
              OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
              OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
              OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
              OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
        UNION ALL
        -- GMP
        SELECT  
            created_at,
            LOWER(data:call.chain::STRING) AS source_chain,
            LOWER(data:call.returnValues.destinationChain::STRING) AS destination_chain,
            data:call.transaction.from::STRING AS transfer_user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            id
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND (
              data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
              OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
              OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
              OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
              OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT source_chain AS "Source Chain", 
           destination_chain AS "Destination Chain",
           ROUND(SUM(amount_usd)) AS "Swap Volume (USD)", 
           COUNT(DISTINCT id) AS "Swap Count", 
           COUNT(DISTINCT transfer_user) AS "Swapper Count"
    FROM axelar_service
    WHERE created_at::date >= '{start_str}' AND created_at::date <= '{end_str}' and amount_usd is not null
    GROUP BY 1, 2;
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data -------------------------------------------------------------------------------------------------
df_flows = load_chain_flows(start_date, end_date)

# --- Build Network Graph --------------------------------------------------------------------------------------
def make_network_chart(df, weight_col, title):
    G = nx.DiGraph()
    for _, row in df.iterrows():
        G.add_edge(row["Source Chain"], row["Destination Chain"], weight=row[weight_col])

    pos = nx.spring_layout(G, k=0.5, seed=42)

    # --- Create edge traces individually ---
    edge_traces = []
    max_weight = df[weight_col].max()
    for u, v, d in G.edges(data=True):
        x0, y0 = pos[u]
        x1, y1 = pos[v]
        weight = max(1, d['weight']/max_weight*10)  # scale width
        trace = go.Scatter(
            x=[x0, x1],
            y=[y0, y1],
            line=dict(width=weight, color="LightSkyBlue"),
            hoverinfo='text',
            mode='lines',
            text=f"{u} → {v}<br>{weight_col}: {d['weight']}"
        )
        edge_traces.append(trace)

    # --- Create node trace ---
    node_x, node_y, text = [], [], []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        text.append(node)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode="markers+text",
        text=text,
        textposition="top center",
        hoverinfo="text",
        marker=dict(size=20, color="orange", line=dict(width=2, color="DarkSlateGrey"))
    )

    fig = go.Figure(data=edge_traces + [node_trace],
                    layout=go.Layout(
                        title=title,
                        showlegend=False,
                        hovermode="closest",
                        margin=dict(b=20, l=5, r=5, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                    ))
    return fig

# --- Tabs for Metrics -----------------------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["Swapper Count", "Swap Count", "Swap Volume"])

with tab1:
    st.plotly_chart(make_network_chart(df_flows, "Swapper Count", "Flows by Swapper Count"), use_container_width=True)

with tab2:
    st.plotly_chart(make_network_chart(df_flows, "Swap Count", "Flows by Swap Count"), use_container_width=True)

with tab3:
    st.plotly_chart(make_network_chart(df_flows, "Swap Volume (USD)", "Flows by Swap Volume"), use_container_width=True)

# --- Query Function: Row (5) --------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_source_chain_data(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
        -- Token Transfers
        SELECT 
            created_at, 
            LOWER(data:send:original_source_chain) AS source_chain, 
            LOWER(data:send:original_destination_chain) AS destination_chain,
            recipient_address AS user, 
            CASE 
              WHEN IS_ARRAY(data:send:amount) THEN NULL
              WHEN IS_OBJECT(data:send:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
              WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
              ELSE NULL
            END AS amount_usd,
            CASE 
              WHEN IS_ARRAY(data:send:fee_value) THEN NULL
              WHEN IS_OBJECT(data:send:fee_value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
              ELSE NULL
            END AS fee,
            id, 
            'Token Transfers' AS Service, 
            data:link:asset::STRING AS raw_asset
        FROM axelar.axelscan.fact_transfers
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND created_at::date >= '{start_str}' 
          AND created_at::date <= '{end_str}'
          AND (
            sender_address ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR sender_address ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR sender_address ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR sender_address ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR sender_address ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )

        UNION ALL

        -- GMP
        SELECT  
            created_at,
            data:call.chain::STRING AS source_chain,
            data:call.returnValues.destinationChain::STRING AS destination_chain,
            data:call.transaction.from::STRING AS user,
            CASE 
              WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
              WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
              ELSE NULL
            END AS amount,
            CASE 
              WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
              WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
              ELSE NULL
            END AS amount_usd,
            COALESCE(
              CASE 
                WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
                  OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
                THEN NULL
                WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
                  AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
                THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
                ELSE NULL
              END,
              CASE 
                WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
                WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
                ELSE NULL
              END
            ) AS fee,
            id, 
            'GMP' AS Service, 
            data:symbol::STRING AS raw_asset
        FROM axelar.axelscan.fact_gmp 
        WHERE status = 'executed'
          AND simplified_status = 'received'
          AND created_at::date >= '{start_str}' 
          AND created_at::date <= '{end_str}'
          AND (
            data:approved:returnValues:contractAddress ilike '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%' 
            OR data:approved:returnValues:contractAddress ilike '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
            OR data:approved:returnValues:contractAddress ilike '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
            OR data:approved:returnValues:contractAddress ilike '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
            OR data:approved:returnValues:contractAddress ilike '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
          )
    )
    SELECT source_chain AS "Source Chain", 
           COUNT(DISTINCT id) AS "Number of Transfers", 
           COUNT(DISTINCT user) AS "Number of Users", 
           ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM axelar_service
    GROUP BY 1
    ORDER BY 2 DESC
    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
df_source = load_source_chain_data(start_date, end_date)

# --- Top 10 Vertical Bar Charts ----------------------------------------------------------------------------------
top_vol = df_source.nlargest(10, "Volume of Transfers (USD)")
top_txn = df_source.nlargest(10, "Number of Transfers")
top_usr = df_source.nlargest(10, "Number of Users")

col1, col2, col3 = st.columns(3)

with col1:
    fig1 = px.bar(
        top_vol.sort_values("Volume of Transfers (USD)", ascending=False),
        x="Source Chain", y="Volume of Transfers (USD)",
        text="Volume of Transfers (USD)",  
        color="Source Chain",  
        title="Top 10 Source Chains by Swap Volume",
        labels={"Volume of Transfers (USD)": "USD", "Source Chain": " "},
    )
    fig1.update_traces(textposition='outside') 
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(
        top_txn.sort_values("Number of Transfers", ascending=False),
        x="Source Chain", y="Number of Transfers",
        text="Number of Transfers",
        color="Source Chain",
        title="Top 10 Source Chains by Swap Count",
        labels={"Number of Transfers": "Txns count", "Source Chain": " "},
    )
    fig2.update_traces(textposition='outside')
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = px.bar(
        top_usr.sort_values("Number of Users", ascending=False),
        x="Source Chain", y="Number of Users",
        text="Number of Users",
        color="Source Chain",
        title="Top 10 Source Chains by Swapper Count",
        labels={"Number of Users": "Address count", "Source Chain": " "},
    )
    fig3.update_traces(textposition='outside')
    st.plotly_chart(fig3, use_container_width=True)

# --- Destination Chain Data Query: Row 6 ---------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_destination_data(start_date, end_date):
    # ensure string format YYYY-MM-DD
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")

    query = f"""
    WITH axelar_service AS (
      SELECT 
        created_at, 
        LOWER(data:send:original_source_chain) AS source_chain, 
        LOWER(data:send:original_destination_chain) AS destination_chain,
        recipient_address AS user, 
        CASE 
          WHEN IS_ARRAY(data:send:amount) THEN NULL
          WHEN IS_OBJECT(data:send:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:send:amount) OR IS_ARRAY(data:link:price) THEN NULL
          WHEN IS_OBJECT(data:send:amount) OR IS_OBJECT(data:link:price) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:amount::STRING) IS NOT NULL AND TRY_TO_DOUBLE(data:link:price::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:send:amount::STRING) * TRY_TO_DOUBLE(data:link:price::STRING)
          ELSE NULL
        END AS amount_usd,
        CASE 
          WHEN IS_ARRAY(data:send:fee_value) THEN NULL
          WHEN IS_OBJECT(data:send:fee_value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:send:fee_value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:send:fee_value::STRING)
          ELSE NULL
        END AS fee,
        id, 
        'Token Transfers' AS "Service", 
        data:link:asset::STRING AS raw_asset
      FROM axelar.axelscan.fact_transfers
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          sender_address ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR sender_address ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR sender_address ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR sender_address ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR sender_address ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )

      UNION ALL

      SELECT  
        created_at,
        data:call.chain::STRING AS source_chain,
        data:call.returnValues.destinationChain::STRING AS destination_chain,
        data:call.transaction.from::STRING AS user,
        CASE 
          WHEN IS_ARRAY(data:amount) OR IS_OBJECT(data:amount) THEN NULL
          WHEN TRY_TO_DOUBLE(data:amount::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:amount::STRING)
          ELSE NULL
        END AS amount,
        CASE 
          WHEN IS_ARRAY(data:value) OR IS_OBJECT(data:value) THEN NULL
          WHEN TRY_TO_DOUBLE(data:value::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:value::STRING)
          ELSE NULL
        END AS amount_usd,
        COALESCE(
          CASE 
            WHEN IS_ARRAY(data:gas:gas_used_amount) OR IS_OBJECT(data:gas:gas_used_amount) 
              OR IS_ARRAY(data:gas_price_rate:source_token.token_price.usd) OR IS_OBJECT(data:gas_price_rate:source_token.token_price.usd) 
            THEN NULL
            WHEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) IS NOT NULL 
              AND TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING) IS NOT NULL 
            THEN TRY_TO_DOUBLE(data:gas:gas_used_amount::STRING) * TRY_TO_DOUBLE(data:gas_price_rate:source_token.token_price.usd::STRING)
            ELSE NULL
          END,
          CASE 
            WHEN IS_ARRAY(data:fees:express_fee_usd) OR IS_OBJECT(data:fees:express_fee_usd) THEN NULL
            WHEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING) IS NOT NULL THEN TRY_TO_DOUBLE(data:fees:express_fee_usd::STRING)
            ELSE NULL
          END
        ) AS fee,
        id, 
        'GMP' AS "Service", 
        data:symbol::STRING AS raw_asset
      FROM axelar.axelscan.fact_gmp 
      WHERE status = 'executed'
        AND simplified_status = 'received'
        AND created_at::date >= '{start_str}'
        AND created_at::date <= '{end_str}'
        AND (
          data:approved:returnValues:contractAddress ILIKE '%0xce16F69375520ab01377ce7B88f5BA8C48F8D666%'
          OR data:approved:returnValues:contractAddress ILIKE '%0x492751eC3c57141deb205eC2da8bFcb410738630%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xDC3D8e1Abe590BCa428a8a2FC4CfDbD1AcF57Bd9%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xdf4fFDa22270c12d0b5b3788F1669D709476111E%'
          OR data:approved:returnValues:contractAddress ILIKE '%0xe6B3949F9bBF168f4E3EFc82bc8FD849868CC6d8%'
        )
    )

    SELECT 
      destination_chain AS "Destination Chain", 
      COUNT(DISTINCT id) AS "Number of Transfers", 
      COUNT(DISTINCT user) AS "Number of Users", 
      ROUND(SUM(amount_usd)) AS "Volume of Transfers (USD)"
    FROM axelar_service
    GROUP BY 1
    ORDER BY "Number of Transfers" DESC
    """

    df = pd.read_sql(query, conn)

    # normalize column names for easier downstream use
    df = df.rename(columns={
        "Destination Chain": "Destination Chain",
        "Number of Transfers": "Number of Transfers",
        "Number of Users": "Number of Users",
        "Volume of Transfers (USD)": "Volume of Transfers (USD)"
    })

    return df

# --- Use the cached loader ---------------------------------------------------------
df_dest = load_destination_data(start_date, end_date)

# --- prepare top-10s and charts (vertical bars) ------------------------------------
top_vol_dest = df_dest.nlargest(10, "Volume of Transfers (USD)").sort_values("Volume of Transfers (USD)", ascending=False)
top_txn_dest = df_dest.nlargest(10, "Number of Transfers").sort_values("Number of Transfers", ascending=False)
top_usr_dest = df_dest.nlargest(10, "Number of Users").sort_values("Number of Users", ascending=False)

# Volume chart
fig_vol_dest = px.bar(
    top_vol_dest,
    x="Destination Chain",
    y="Volume of Transfers (USD)",
    text="Volume of Transfers (USD)", 
    color="Destination Chain",         
    title="Top 10 Destination Chains by Swap Volume",
    labels={"Volume of Transfers (USD)": "USD", "Destination Chain": " "},
)
fig_vol_dest.update_traces(textposition='outside')  
fig_vol_dest.update_yaxes(tickformat=",.0f")  

# Transfers chart
fig_txn_dest = px.bar(
    top_txn_dest,
    x="Destination Chain",
    y="Number of Transfers",
    text="Number of Transfers",
    color="Destination Chain",
    title="Top 10 Destination Chains by Swap Count",
    labels={"Number of Transfers": "Txns count", "Destination Chain": " "},
)
fig_txn_dest.update_traces(textposition='outside')
fig_txn_dest.update_yaxes(tickformat=",.0f")

# Users chart
fig_usr_dest = px.bar(
    top_usr_dest,
    x="Destination Chain",
    y="Number of Users",
    text="Number of Users",
    color="Destination Chain",
    title="Top 10 Destination Chains by Swapper count",
    labels={"Number of Users": "Addresses count", "Destination Chain": " "},
)
fig_usr_dest.update_traces(textposition='outside')
fig_usr_dest.update_yaxes(tickformat=",.0f")

# --- display three charts in one row -----------------------------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.plotly_chart(fig_vol_dest, use_container_width=True)
with col2:
    st.plotly_chart(fig_txn_dest, use_container_width=True)
with col3:
    st.plotly_chart(fig_usr_dest, use_container_width=True)

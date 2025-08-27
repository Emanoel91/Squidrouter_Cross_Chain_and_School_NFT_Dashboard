import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import requests

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Squidrouter: Cross Chain & School NFT Dashboard",
    page_icon="https://img.cryptorank.io/coins/squid1675241862798.png",
    layout="wide"
)

# --- Title ------------------------------------------------------------------------------------------------------------
st.markdown("""
<h1 style='text-align: center; color: #f1c40f;'>🟡 Squid Scholar NFT Dashboard</h1>
""", unsafe_allow_html=True)

# --- Attention --------------------------------------------------------------------------------------------------------
st.info("📊Tables initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Sidebar Footer Slightly Left-Aligned -----------------------------------------------------------------------------
st.sidebar.markdown(
    """
    <style>
    .sidebar-footer {
        position: fixed;
        bottom: 20px;
        width: 250px;
        font-size: 13px;
        color: gray;
        margin-left: 5px;
        text-align: left;  
    }
    .sidebar-footer img {
        width: 16px;
        height: 16px;
        vertical-align: middle;
        border-radius: 50%;
        margin-right: 5px;
    }
    .sidebar-footer a {
        color: gray;
        text-decoration: none;
    }
    </style>

    <div class="sidebar-footer">
        <div>
            <a href="https://x.com/axelar" target="_blank">
                <img src="https://img.cryptorank.io/coins/axelar1663924228506.png" alt="Axelar Logo">
                Powered by Axelar
            </a>
        </div>
        <div style="margin-top: 5px;">
            <a href="https://x.com/0xeman_raz" target="_blank">
                <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" alt="Eman Raz">
                Built by Eman Raz
            </a>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Snowflake Connection ---------------------------------------------------------------------------------------------
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

# --- Helper function to load data from API ----------------------------------------------------------------------------
def load_api(url):
    r = requests.get(url)
    r.raise_for_status()
    return r.json()["result"]["rows"]

# --- KPI Section ------------------------------------------------------------------------------------------------------
# Total Minters
url1 = "https://api.dune.com/api/v1/query/5693825/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
df1 = pd.DataFrame(load_api(url1))
kpi1 = df1["Total Minters"].iloc[0]

# Total NFTs Minted
url2 = "https://api.dune.com/api/v1/query/5693849/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
df2 = pd.DataFrame(load_api(url2))
kpi2 = df2["Total NFTs Minted"].iloc[0]

# Total Value of NFTs Minted
url3 = "https://api.dune.com/api/v1/query/5693983/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
df3_kpi = pd.DataFrame(load_api(url3))
kpi3 = df3_kpi["Total Value of NFTs Minted"].iloc[-1]

col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f"""
    <div style="background-color:#27ae60; padding:20px; border-radius:15px; text-align:center;">
    <h3 style="color:white;">Total Minters</h3>
    <h2 style="color:white;">{kpi1:,}</h2>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div style="background-color:#2980b9; padding:20px; border-radius:15px; text-align:center;">
    <h3 style="color:white;">Total NFTs Minted</h3>
    <h2 style="color:white;">{kpi2:,}</h2>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div style="background-color:#f39c12; padding:20px; border-radius:15px; text-align:center;">
    <h3 style="color:white;">Total Value of NFTs Minted ($)</h3>
    <h2 style="color:white;">${kpi3:,.0f}</h2>
    </div>
    """, unsafe_allow_html=True)

# --- Filter Date up to 2024-08-17 -------------------------------------------------------------------------------------
cutoff_date = pd.to_datetime("2024-08-17")

# --- 3: Number of NFTs Minted per Day ---------------------------------------------------------------------------------
url4 = "https://api.dune.com/api/v1/query/5693886/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
df4 = pd.DataFrame(load_api(url4))
df4["Date"] = pd.to_datetime(df4["Date"])
df4 = df4[df4["Date"] <= cutoff_date]

fig1 = go.Figure()
fig1.add_bar(x=df4["Date"], y=df4["Number of NFT Minted"], name="Number of NFT Minted", yaxis="y1", marker_color="#3498db")
fig1.add_trace(go.Scatter(x=df4["Date"], y=df4["Total Number of NFT Minted"], name="Total Number of NFT Minted", yaxis="y2", mode="lines+markers", line=dict(color="#2ecc71", width=2)))

fig1.update_layout(
    title="Number of NFTs Minted per Day",
    xaxis=dict(title="Date"),
    yaxis=dict(title="Number of NFT Minted", side="left"),
    yaxis2=dict(title="Total Number of NFT Minted", overlaying="y", side="right"),
    template="plotly_white"
)

# --- 4: Value of NFTs Minted per Day ----------------------------------------------------------------------------------
url5 = "https://api.dune.com/api/v1/query/5693983/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
df5 = pd.DataFrame(load_api(url5))
df5["Date"] = pd.to_datetime(df5["Date"])
df5 = df5[df5["Date"] <= cutoff_date]

fig2 = go.Figure()
fig2.add_bar(x=df5["Date"], y=df5["Value of NFTs Minted"], name="Value of NFTs Minted", yaxis="y1", marker_color="#9b59b6")
fig2.add_trace(go.Scatter(x=df5["Date"], y=df5["Total Value of NFTs Minted"], name="Total Value of NFTs Minted", yaxis="y2", mode="lines+markers", line=dict(color="#e74c3c", width=2)))

fig2.update_layout(
    title="Value of NFTs Minted per Day",
    xaxis=dict(title="Date"),
    yaxis=dict(title="Value of NFTs Minted ($)", side="left"),
    yaxis2=dict(title="Total Value of NFTs Minted ($)", overlaying="y", side="right"),
    template="plotly_white"
)

col4, col5 = st.columns(2)
with col4:
    st.plotly_chart(fig1, use_container_width=True)
with col5:
    st.plotly_chart(fig2, use_container_width=True)

# --- 5: Table Number of NFT Minted vs Minters -------------------------------------------------------------------------
url6 = "https://api.dune.com/api/v1/query/5693905/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
df6 = pd.DataFrame(load_api(url6))
df6.index = df6.index + 1  # index start from 1

# --- 6: Table Addresses and NFTs Minted -------------------------------------------------------------------------------
url7 = "https://api.dune.com/api/v1/query/5694318/results?api_key=kmCBMTxWKBxn6CVgCXhwDvcFL1fBp6rO"
df7 = pd.DataFrame(load_api(url7))
df7 = df7.rename(columns={"to": "Address"})
df7.index = df7.index + 1

col6, col7 = st.columns(2)
with col6:
    st.subheader("📋 Number of NFT Minted vs Minters")
    st.dataframe(df6, use_container_width=True, height=200)  # ~ 5 rows

with col7:
    st.subheader("📋 Top Addresses by NFT Minted")
    st.dataframe(df7, use_container_width=True, height=200)  # ~ 5 rows

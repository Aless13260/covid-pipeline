import streamlit as st
st.set_page_config(layout="wide")

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pycountry   # 依赖已加入 requirements.txt

# ----------------------------
# Load & cache
# ----------------------------
@st.cache_data
def load_data():
    df = pd.read_parquet("data.parquet")
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df

df = load_data()

# ▸ 生成合法国家名单（ISO-3166）
valid_countries = {c.name for c in pycountry.countries}

st.title("🌍 Global COVID-19 Data Analysis Dashboard")

# ----------------------------
# Tabs
# ----------------------------
tab1, tab2, tab3 = st.tabs(["🌍 Global Summary", "🚨 Anomaly Detection", "📈 Daily Trends"])

# ----------------------------
# TAB 1 : Global Summary
# ----------------------------
with tab1:
    st.header("🌍 Cumulative Confirmed and Deaths Overview")

    # 1 过滤出真正国家行 + 确诊>0
    df_valid = df[df["country"].isin(valid_countries) & (df["confirmed"] > 0)]

    # 2 取每国最新一日
    latest_per_country = (
        df_valid
        .sort_values("date")
        .groupby("country", as_index=False)
        .last()
    )
    latest_date = latest_per_country["date"].max().strftime("%Y-%m-%d")
    st.caption(f"📅 Data last updated: {latest_date}")

    # 3 全球累计卡片
    total_confirmed = int(latest_per_country["confirmed"].sum())
    total_deaths    = int(latest_per_country["deaths"].sum())

    c1, c2 = st.columns(2)
    c1.metric("Total Confirmed Cases", f"{total_confirmed:,}")
    c2.metric("Total Deaths", f"{total_deaths:,}")

    # 4 世界地图
    map_fig = px.choropleth(
        latest_per_country,
        locations="country",
        locationmode="country names",
        color="confirmed",
        color_continuous_scale="Reds",
        title="Confirmed Cases Worldwide",
        height=500
    )
    map_fig.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    st.plotly_chart(map_fig, use_container_width=True)

    # 5 Yearly trend（双 Y 轴）
    df_valid["year"] = df_valid["date"].dt.year
    yearly_last = (
        df_valid
        .sort_values("date")
        .groupby(["country", "year"], as_index=False)
        .last()
    )
    yearly_sum = yearly_last.groupby("year")[["confirmed", "deaths"]].sum().reset_index()

    line = go.Figure()

    # 左轴：累计确诊
    line.add_scatter(
        x=yearly_sum["year"],
        y=yearly_sum["confirmed"],
        name="Confirmed",
        mode="lines+markers",
        yaxis="y1"
    )

    # 右轴：累计死亡
    line.add_scatter(
        x=yearly_sum["year"],
        y=yearly_sum["deaths"],
        name="Deaths",
        mode="lines+markers",
        yaxis="y2"
    )

    line.update_layout(
        title="Yearly Cumulative Confirmed vs Deaths",
        xaxis_title="Year",
        yaxis=dict(title="Confirmed Cases", side="left"),
        yaxis2=dict(
            title="Deaths",
            overlaying="y",
            side="right",
            showgrid=False
        ),
        legend=dict(x=0.01, y=0.99)
    )

    st.plotly_chart(line, use_container_width=True)

# ----------------------------
# TAB 2 : Anomaly Detection
# ----------------------------
with tab2:
    st.header("🚨 Country-Level Anomaly Detection")
    countries_with_data = df_valid[df_valid["daily_new_cases"] > 0]["country"].unique()
    sel_country = st.selectbox("Select Country", sorted(countries_with_data))

    df_c = df_valid[df_valid["country"] == sel_country]
    fig_box = px.box(
        df_c,
        x="country",
        y="daily_new_cases",
        points="all",
        title="Daily New Cases Distribution (Outliers Highlighted)"
    )
    st.plotly_chart(fig_box, use_container_width=True)

# ----------------------------
# TAB 3 : Daily Trends & Growth
# ----------------------------
with tab3:
    st.header("📈 Daily Trends and Growth Rate")
    sel_country2 = st.selectbox(
        "Country for Trend",
        sorted(countries_with_data),
        key="trend"
    )

    df_c = df_valid[df_valid["country"] == sel_country2].copy()
    df_c = df_c.sort_values("date")
    df_c["growth_rate"] = df_c["confirmed"].pct_change().fillna(0)

    fig = go.Figure()
    fig.add_scatter(
        x=df_c["date"],
        y=df_c["daily_new_cases"],
        mode="lines",
        name="Daily New Cases"
    )
    fig.add_scatter(
        x=df_c["date"],
        y=df_c["growth_rate"] * 100,
        mode="lines",
        name="Growth Rate %",
        yaxis="y2"
    )

    fig.update_layout(
        title=f"Daily New Cases & Growth Rate – {sel_country2}",
        xaxis_title="Date",
        yaxis=dict(title="New Cases"),
        yaxis2=dict(
            title="Growth Rate (%)",
            overlaying="y",
            side="right"
        ),
        legend=dict(x=0.01, y=0.99)
    )
    st.plotly_chart(fig, use_container_width=True)




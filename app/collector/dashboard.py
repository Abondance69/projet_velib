import streamlit as st
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
import plotly.express as px

# ---------------- CONFIG ----------------
st.set_page_config(page_title="Vélib Dashboard (MongoDB)", layout="wide")

MONGO_URI = "mongodb://admin:pwd@localhost:27017/"
DB_NAME = "velib"
COLLECTION = "velib_stream_api"

# ---------------- LOAD DATA ----------------
@st.cache_data(ttl=10)
def load_data():
    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLLECTION]
    data = list(coll.find({}, {"_id": 0}))
    df = pd.DataFrame(data)

    if "window" in df.columns:
        win = pd.json_normalize(df["window"])
        win.columns = ["window.start", "window.end"]
        df = pd.concat([df.drop(columns=["window"]), win], axis=1)

    for c in ["avg_available_bikes", "avg_available_stands"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["window.start"] = pd.to_datetime(df["window.start"], utc=True)
    return df

df = load_data()

st.title("🚲 Dashboard Vélib – MongoDB (Streaming)")
st.caption("Données temps réel ingérées par Spark Streaming")

# ---------------- FILTERS ----------------
stations = ["Toutes"] + sorted(df["name"].dropna().unique())
station = st.selectbox("Station", stations)

if station != "Toutes":
    df = df[df["name"] == station]

# ---------------- KPIs ----------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Stations", df["stationcode"].nunique())
c2.metric("Vélos dispo", int(df["avg_available_bikes"].sum()))
c3.metric("Bornes libres", int(df["avg_available_stands"].sum()))
c4.metric(
    "Taux vélos",
    f"{df['avg_available_bikes'].sum() / (df['avg_available_bikes'].sum() + df['avg_available_stands'].sum()):.1%}"
)

st.divider()

# ---------------- GRAPH 1 : TIME SERIES ----------------
st.subheader("📈 Évolution des vélos et bornes dans le temps")
agg_time = df.groupby("window.start", as_index=False).mean(numeric_only=True)

fig1 = px.line(
    agg_time,
    x="window.start",
    y=["avg_available_bikes", "avg_available_stands"],
    labels={"value": "Moyenne", "window.start": "Temps"}
)
st.plotly_chart(fig1, use_container_width=True)

# ---------------- GRAPH 2 : TOP STATIONS ----------------
st.subheader("🏆 Top stations (vélos disponibles)")
top = (
    df.groupby("name", as_index=False)["avg_available_bikes"]
    .mean()
    .sort_values("avg_available_bikes", ascending=False)
    .head(10)
)

fig2 = px.bar(
    top,
    x="avg_available_bikes",
    y="name",
    orientation="h",
    labels={"avg_available_bikes": "Vélos moyens", "name": "Station"}
)
st.plotly_chart(fig2, use_container_width=True)

# ---------------- GRAPH 3 : STACKED BAR ----------------
st.subheader("🧱 Vélos vs Bornes par station (empilé)")
stack = df.groupby("name", as_index=False).mean(numeric_only=True)
stack = stack.head(20)

stack_long = stack.melt(
    id_vars="name",
    value_vars=["avg_available_bikes", "avg_available_stands"],
    var_name="Type",
    value_name="Valeur"
)

fig3 = px.bar(
    stack_long,
    x="name",
    y="Valeur",
    color="Type",
    barmode="stack"
)
fig3.update_layout(xaxis_tickangle=-45)
st.plotly_chart(fig3, use_container_width=True)

# ---------------- GRAPH 4 : HEATMAP ----------------
st.subheader("🗓️ Heatmap – vélos moyens par heure")
df["hour"] = df["window.start"].dt.hour

heat = df.pivot_table(
    index="name",
    columns="hour",
    values="avg_available_bikes",
    aggfunc="mean"
)

fig4 = px.imshow(
    heat.head(15),
    aspect="auto",
    labels={"x": "Heure", "y": "Station", "color": "Vélos moyens"}
)
st.plotly_chart(fig4, use_container_width=True)

# ---------------- TABLE ----------------
st.subheader("🧾 Données brutes")
st.dataframe(df.head(500))

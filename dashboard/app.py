"""
dashboard/app.py — Streamlit Enforcer Dashboard
================================================
Visualises live enforcement data. Reads files only — never re-runs
enforcement logic. Run with:

    streamlit run dashboard/app.py
"""
import streamlit as st

st.set_page_config(page_title="Data Contract Enforcer", layout="wide")
st.title("Data Contract Enforcer")
st.info("No data yet — run the pipeline first.")

# TODO: implement panels

import os
import sys
from pathlib import Path
import streamlit as st

# Path setup so we can import local modules
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "AIAgentForCityGML"))

from AIAgentForCityGML.agent_manager import AgentManager
from ai_report_generator import generate_report

RESULT_PDF = ROOT / "result.pdf"
TEMPLATE_PATH = ROOT / "prompt_template.txt"

st.set_page_config(page_title="AI レポート ビューワー", layout="wide")
st.title("AI レポート ビューワー")

with st.form("report_form"):
    st.subheader("入力")
    purposes = st.text_area("レポートの目的（複数行可・1行1項目）", height=160, value="")
    areas = st.text_area("対象地域（複数行可・1行1地域）", height=120, value="")
    submitted = st.form_submit_button("PDF を生成")

if submitted:
    # Build prompt from template
    with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
        template = f.read()
    purposes_list = [p.strip() for p in purposes.splitlines() if p.strip()]
    areas_list = [a.strip() for a in areas.splitlines() if a.strip()]
    purpose_string = "\n".join([f"- {d}" for d in purposes_list])
    prompt = template.replace("{{PURPOSE_LIST}}", purpose_string)
    target_area_string = ", ".join([f"{d}" for d in areas_list])
    prompt = prompt.replace("{{TARGET_AREA}}", target_area_string)

    # Query and generate
    agent = AgentManager()
    with st.spinner("レポート生成中..."):
        response = agent.query(prompt)
        generate_report(response, str(RESULT_PDF))
    st.success("PDF を生成しました")

# Viewer
st.subheader("PDF プレビュー")
if RESULT_PDF.exists():
    with open(RESULT_PDF, "rb") as f:
        st.download_button("PDF をダウンロード", data=f, file_name="result.pdf", mime="application/pdf")
    # Use iframe to embed PDF
    pdf_url = f"file:///{RESULT_PDF.as_posix()}"
    st.markdown(
        f'<iframe src="{pdf_url}" width="100%" height="800px" style="border:1px solid #ddd;border-radius:8px;"></iframe>',
        unsafe_allow_html=True,
    )
else:
    st.info("PDF がまだありません。フォームから生成してください。")

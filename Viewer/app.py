import os
import sys
from pathlib import Path
from flask import Flask, render_template, request, send_file, url_for, redirect

# Ensure project root is on sys.path for imports
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "AIAgentForCityGML"))

# Reuse existing modules (after path setup)
from AIAgentForCityGML.agent_manager import AgentManager
from ai_report_generator import generate_report

app = Flask(__name__)

# Place result.pdf at repo root (same as ai_report_generator)
OUTPUT_PDF = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'result.pdf'))
TEMPLATE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'prompt_template.txt'))

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/generate', methods=['POST'])
def generate():
    # Inputs from form (multi-line)
    purposes = [p.strip() for p in request.form.get('purposes', '').splitlines() if p.strip()]
    areas = [a.strip() for a in request.form.get('areas', '').splitlines() if a.strip()]

    # Build prompt similar to get_prompt()
    with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
        template = f.read()
    purpose_string = '\n'.join([f'- {d}' for d in purposes])
    prompt = template.replace('{{PURPOSE_LIST}}', purpose_string)
    target_area_string = ', '.join([f"{d}" for d in areas])
    prompt = prompt.replace('{{TARGET_AREA}}', target_area_string)

    # Query agent and generate PDF
    agent = AgentManager()
    response = agent.query(prompt)
    generate_report(response, OUTPUT_PDF)

    return redirect(url_for('view'))

@app.route('/view', methods=['GET'])
def view():
    exists = os.path.exists(OUTPUT_PDF)
    return render_template('view.html', exists=exists, pdf_url=url_for('pdf'))

@app.route('/pdf', methods=['GET'])
def pdf():
    if not os.path.exists(OUTPUT_PDF):
        return 'PDF not found', 404
    return send_file(OUTPUT_PDF, mimetype='application/pdf')

if __name__ == '__main__':
    port = int(os.getenv('PORT', '5000'))
    app.run(host='127.0.0.1', port=port, debug=True)

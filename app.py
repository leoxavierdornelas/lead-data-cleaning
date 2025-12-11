from flask import Flask, render_template, request
import pandas as pd
import os
from src.pipeline import processar_leads, gerar_metricas

app = Flask(__name__)

UPLOAD_FOLDER = 'data'
OUTPUT_FOLDER = 'outputs'

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            filepath = os.path.join(UPLOAD_FOLDER, file.filename)
            file.save(filepath)

            df_raw = pd.read_csv(filepath)
            df_qual, df_inv = processar_leads(df_raw)
            metrics = gerar_metricas(df_raw, df_qual, df_inv)

            # Salva outputs
            df_qual.to_csv(os.path.join(OUTPUT_FOLDER, 'leads_qualificados.csv'), index=False)
            df_inv.to_csv(os.path.join(OUTPUT_FOLDER, 'leads_invalidos.csv'), index=False)

            return render_template('results.html', metrics=metrics,
                                   qualificados=df_qual.to_dict(orient='records'),
                                   invalidos=df_inv.to_dict(orient='records'))
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)

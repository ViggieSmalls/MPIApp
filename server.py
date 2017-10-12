from flask import Flask, jsonify, render_template
import pandas as pd
import os
from math import floor, ceil
import numpy as np

app = Flask(__name__)

@app.route('/')
def home():
    projects = os.listdir(os.path.join(app.static_folder, 'data'))
    return render_template('start.html', projects=projects)

@app.route('/<project_name>/')
def project(project_name):
    return render_template('main.html', project_name=project_name)

@app.route('/<project_name>/data/')
def project_data(project_name):
    process_table = os.path.join(app.static_folder, 'data', project_name, 'process_table.csv')
    df = pd.read_csv(process_table)
    df['Defocus'] = df[["Defocus_U", "Defocus_V"]].mean(axis=1)
    df[['Defocus', 'Defocus_U', 'Defocus_V']] = df[['Defocus', 'Defocus_U', 'Defocus_V']] / 1000
    df['delta_Defocus'] = df["Defocus_U"] - df["Defocus_V"]
    defocus_max = ceil(df['Defocus'].max())
    defocus_min = floor(df['Defocus'].min())
    defocus_bins = list(np.arange(defocus_min - 0.5, defocus_max + 0.5, 1))
    defocus_bars = pd.cut(df['Defocus'], defocus_bins)

    res_list = df['Resolution'].tolist()
    bars, bins = np.histogram(res_list)
    res_bins = np.mean(np.column_stack((bins[:-1], bins[1:])), axis=1)
    res_bars = bars

    timeline_data = {
        'labels': df['Unnamed: 0'].tolist(),
        'delta_Defocus': [round(x,2) for x in df['delta_Defocus'].tolist()],
        'Phase_shift': df['Phase_shift'].tolist(),
        'Defocus': [round(x,2) for x in df['Defocus'].tolist()]
    }

    defocus_barchart = {
        'bins': list(map(lambda x: x + 0.5, defocus_bins)),
        'data': pd.value_counts(defocus_bars, sort=False).tolist()
    }

    res_barchart = {
        'bins': res_bins.tolist(),
        'data': res_bars.tolist()
    }

    data = df.round(2).to_dict(orient='records')

    for dic in [timeline_data, defocus_barchart, res_barchart]:
        for k, v in dic.items():
            v.insert(0, k)

    return jsonify({'data': data, 'timeline': timeline_data, 'defocus': defocus_barchart, 'resolution': res_barchart})


if __name__ == "__main__":
    app.run()

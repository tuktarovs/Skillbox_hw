# <YOUR_IMPORTS>
import dill
import glob
import os
import json
import datetime
import pandas as pd
from datetime import datetime


def predict():
    # <YOUR_CODE>


    path = os.path.expanduser('~/airflow_hw')
    with open(f'{path}/data/models/cars_pipe_202303061734.pkl', 'rb') as file:
        model = dill.load(file)

    predicted_df = pd.DataFrame(columns=['id', 'predict'])

    def prediction(data):
        y = model.predict(df)[0]
        predicted_df.loc[len(predicted_df)] = [int(data.id), y]

    path_files = path + '/data/test/*json'
    for json_file in glob.iglob(path_files):
        with open(json_file) as f:
            df = pd.DataFrame.from_dict([dict(json.load(f))])
        prediction(df)

    predicted_df.to_csv(f'{path}/data/predictions/Prediction_{datetime.now().strftime("%y%m%d%H%M")}.csv')


if __name__ == '__main__':
    predict()

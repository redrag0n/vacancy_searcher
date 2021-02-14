import os
import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go


def count_mean_salary(salary_record):
    if salary_record is None:
        return None
    if isinstance(salary_record, dict):
        if salary_record['currency'] != 'RUR':  # todo: обработка зарплаты в валюте
            return None
        if (salary_record['gross'] is not None) and (not isinstance(salary_record['gross'], bool)):
            return salary_record['gross']
        if salary_record['from'] is None or salary_record['to'] is None:
            return None
        return salary_record['from'] + (salary_record['to'] - salary_record['from']) / 2
    else:
        print('Unknown salary type')
        return None


def get_key_skill_list(skill_dict_list):
    skill_list = list(map(lambda x: x['name'], skill_dict_list))
    return skill_list


def create_skill_salary_series(vacancy_df):
    skill_set = set(sum(vacancy_df['skill_list'], []))
    skill_salary_dict = dict()
    for skill in skill_set:
        salaries = vacancy_df[vacancy_df['skill_list'].map(lambda x: skill in x)]['salary_mean']
        salaries = salaries.dropna().values
        skill_salary_dict[skill] = salaries
    return pd.Series(skill_salary_dict)


def draw_skill_salary_box_plots(vacancy_df, output_path, top=10, min_count=20):
    skill_salary_series = create_skill_salary_series(vacancy_df)
    skill_salary_series = skill_salary_series[skill_salary_series.map(len) > min_count]
    ordered_skill_indexes = skill_salary_series.map(np.median).sort_values(ascending=False).index
    skill_salary_series = skill_salary_series.loc[ordered_skill_indexes]

    fig = go.Figure()
    for i in range(top):
        fig.add_trace(go.Box(y=skill_salary_series.iloc[i],
                             name=skill_salary_series.index[i]))
    fig.update_traces(boxpoints='all', jitter=0)
    fig.write_html(output_path)
    fig.show()


def read_vacancy_df(input_dir):
    vacancy_list = list()
    for fn in os.listdir(input_dir):
        if not fn.startswith('vacancies'):
            continue
        with open(os.path.join(input_dir, fn), encoding='utf8') as in_:
            vacancy_list.extend(json.load(in_))
    vacancy_df = pd.DataFrame(vacancy_list)
    vacancy_df = vacancy_df.drop_duplicates('id')
    vacancy_df.set_index('id', inplace=True)
    vacancy_df['salary_mean'] = vacancy_df['salary'].map(count_mean_salary)
    vacancy_df['skill_list'] = vacancy_df['key_skills'].map(get_key_skill_list)
    return vacancy_df


if __name__ == '__main__':
    input_dir = 'data'
    output_path = 'data/skill_salary_box_plots.html'
    vacancy_df = read_vacancy_df(input_dir)
    draw_skill_salary_box_plots(vacancy_df, output_path)




